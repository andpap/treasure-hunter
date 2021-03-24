(ns core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [slingshot.slingshot :as sl :refer [throw+ try+]]
            [clojure.core.async :as async :refer [chan go-loop <! >! thread timeout go]])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def base-url (str "http://" (or (System/getenv "ADDRESS") "localhost") ":8000"))

(def STEP1 20)
(def CASH_PARALLELS 2)
(def EXPLORE1_PARALLELS 2)
(def EXPLORE2_PARALLELS 8)
(def DIG_PARALLELS 4)

(defn <-area-queue
  [*amount-area-map]
  (let [queue (->> @*amount-area-map
                   (filter (fn [[amount ^LinkedBlockingQueue queue]]
                             (not (.isEmpty queue))))
                   (map (fn [[_ q]] q))
                   (first))]
    (when queue
      (.poll ^LinkedBlockingQueue queue))))

(defn ->area-queue
  [*amount-areas-map report]
  (when (> (:amount report) 0)
    (swap! *amount-areas-map
           (fn [m amount area]
             (let [queue (or (m amount) (java.util.concurrent.LinkedBlockingQueue.))
                   _     (.add queue area)
                   m     (assoc m amount queue)]
               m))
           (:amount report)
           report)))

(def beging-time (System/currentTimeMillis))

(defn getreq [path]
  (try
    (println "path:" path)
    (let [resp (client/get (str base-url "/" path)
                           {:as :json
                            :content-type :json
                            :retry-handler (fn [ex try-count http-context]
                                             (if (> try-count 2) false true))
                            :accept :json})]
      (println "resp:" resp)
      (:body resp))
    (catch Exception e
      [])))

(defn postreq [path data success raise]
  (client/post (str base-url path)
               {:as :json
                :content-type :json
                :accept :json
                :socket-timeout 10
                :connection-timeout 10
                :body (json/encode data)
                :async? true}
               success
               raise))

(defn postreq-explore [data success raise]
  (postreq "/explore"
           data
           success
           (fn [ex]
             (postreq "/explore"
                      data
                      success
                      raise))))

(defn postreq-dig [data success raise]
  (postreq "/dig" data success raise))

(defn postreq-license [data success raise]
  (postreq "/licenses" data success raise))

(defn postreq-cash [data success raise]
  (postreq "/cash"
           data
           success
           (fn [ex]
             (postreq "/cash"
                      data
                      success
                      raise))))

(def *licenses (atom {}))
(def *coins (atom '()))
(def *explore-reports-map (atom (sorted-map-by >)))
(def *explore-reports-for-dig (atom (sorted-map-by >)))
(def *stat (atom {}))
(def *cache-thread-licenses (ref {}))

(comment
  (count @*licenses)
  (count @*coins)

  )

(defn expired-license?
  [[id license]]
  (let [{:keys [digAllowed digUsed]} license]
    (= digAllowed digUsed)))

(defn remove-expired-licenses []
  (when-let [ids-to-delete (seq (->> @*licenses
                                     (filter expired-license?)
                                     (map #(first %))))]
    (swap! *licenses (partial apply dissoc) ids-to-delete)))

(defn get-license-id []
  (remove-expired-licenses)
  (let [thread-name (.getName (Thread/currentThread))]
    (dosync
     (alter *cache-thread-licenses dissoc thread-name)     
     (let [busy-license-ids (into #{} (vals @*cache-thread-licenses))
           unbusy-license-id   (->> @*licenses
                                 (filter (fn [[k v]] (not (busy-license-ids k))))
                                 (first)
                                 (first))]
       (alter *cache-thread-licenses assoc thread-name unbusy-license-id)
       unbusy-license-id))))

(defn buy-license-promise
  ([]
   (buy-license-promise []))
  ([data]
   (let [result (promise)]
     (postreq-license data
                      (fn [{:keys [status body request-time]}]
                        (deliver result (assoc body :request-time request-time)))
                      (fn [ex]
                        (let [tho (slingshot.slingshot/get-thrown-object ex)
                              status (:status tho)]
                          (deliver result nil))))
     result)))

(defn dig [{:keys [total-amount posX posY]} ch]
  (loop [depth             1
         remaining-amount total-amount]
    (when (and (< depth 11) (> remaining-amount 0))
      (if-let [license-id (get-license-id)]
        (let [result  (promise)]
          (postreq-dig {:licenseID license-id
                        :posX posX
                        :posY posY
                        :depth depth}
                       (fn [{:keys [status body request-time]}]
                                        ;                         (println "dig:" depth "->" request-time)
                         (swap! *stat update (str "dig" depth) #(float (/ (+ (or % request-time) request-time) 2)))
                         (swap! *stat update :treasure-count (fnil inc 0))
                         (deliver result {:status status
                                          :body body}))
                       (fn [ex]
                         (let [tho (slingshot.slingshot/get-thrown-object ex)]
                           (deliver result tho))))
          (let [{:keys [status body]} @result]
            (cond
              (nil? status) nil
              (== 200 status) (do
                                (swap! *licenses update-in [license-id :digUsed] inc)
                                (doseq [treasure-id body]
                                  (async/put! ch {:id treasure-id
                                                  :depth depth}))
                                (recur (inc depth) (- remaining-amount (count body))))
              (== 404 status) (do
                                (swap! *licenses update-in [license-id :digUsed] inc)
                                (recur (inc depth) remaining-amount))
              :default        (do
                                (println "repead dig status " status)
                                (recur depth remaining-amount)))))
        (recur depth remaining-amount)))))

(def *licenses-price-stat (atom (sorted-map)))
(def *last-price (atom 0))

(def get-price (fn []
                        (if (> (count @*licenses-price-stat) 50)
                          (let [[rate price] (first @*licenses-price-stat)]
                            price)
                          @*last-price)))



(defn print-stat []
  (future
    (loop []
      (Thread/sleep (* 1000 60))
      (println "license price:" (get-price))
      (println "licenses count:" (count @*licenses))
      (clojure.pprint/pprint @*licenses)
      (println "map for dig" *explore-reports-for-dig)
      (clojure.pprint/pprint @*stat)      
      (recur))))

(comment
  (get-price)
  (println @*licenses-price-stat)
  (count @*licenses-price-stat)
  (== 200 nil)
  )



(defn run []
  (let [ch-areas (chan 10)
        ch-reports (chan 10)
        ch-reports-finish (chan 10)
        ch-treasures (chan 10)
       ]

    (thread
      (let [update-stat (fn [coins license]
                          (when-not (> (count @*licenses-price-stat) 50)                            
                            (when (= (count coins) @*last-price)
                              (swap! *last-price inc))
                            (when (> (count coins) 0)
                              (swap! *licenses-price-stat
                                     assoc
                                     (- (* 1000 (/ (:digAllowed license) (count coins)))
                                        (:request-time license))                                     
                                     (count coins)))))]
        (loop []
          (remove-expired-licenses)
          (when (< (count @*licenses) 10)
            (let [price (get-price)
                  coins (take price @*coins)
                  _     (swap! *coins #(drop price %))]
              (when-some [license @(buy-license-promise coins)]
                (swap! *licenses assoc (:id license) license)
                (update-stat coins license)))
            )
          
          ;(Thread/sleep 5000)
          (recur))))

    (async/pipeline-async EXPLORE1_PARALLELS
                          ch-reports
                          (fn [area ch]
                            (postreq-explore area
                                             (fn [{:keys [body]}]
                                               (go
                                                 (when (> (:amount body) 1)
                                                   (>! ch body))
                                                 (async/close! ch)))
                                             (fn [ex]
                                            ;   (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                            ;     (println "explore= status:" (:status tho) "message:" (:message tho)))
                                               (async/close! ch))))
                          ch-areas)

    (go
      (doseq [x (range 0 3500 STEP1)
              y (range 0 3500 STEP1)]
        (>! ch-areas {:posX x :posY y :sizeX STEP1 :sizeY STEP1})))

    (go-loop []
      (when-some [report (<! ch-reports)]
        (->area-queue *explore-reports-map
                      report)
        (recur)))

    (go-loop []
      (if-let [report (<-area-queue *explore-reports-map)]
        (let [{:keys [amount area]} report
              startX                (:posX area)
              startY                (:posY area)
              ch-areas              (chan 10)
              *total-amount         (atom amount)
              handler               (fn [area ch]
                                      (if (<= @*total-amount 0)
                                        (async/close! ch)
                                        (postreq-explore area
                                                         (fn [{:keys [body]}]
                                                           (go
                                                             (when (> (:amount body) 0)
                                                               (swap! *total-amount - (:amount body))
                                                               (>! ch body))
                                                             (async/close! ch)))
                                                         (fn [ex]
                                                          ; (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                                          ;   (println "explore1= status:" (:status tho) "message:" (:message tho)))
                                                           (async/close! ch)))))]
          (async/pipeline-async EXPLORE2_PARALLELS ch-reports-finish handler ch-areas)

          (doseq [x (range startX (+ startX (:sizeX area)) 1)
                  y (range startY (+ startY (:sizeY area)) 1)]
            (>! ch-areas {:posX x :posY y :sizeX 1 :sizeY 1}))
          (recur))
        (recur)))

    (go-loop []
      (if-let [report (<! ch-reports-finish)]
        (->area-queue *explore-reports-for-dig report))
      (recur))

    (dotimes [n DIG_PARALLELS]
      (thread
        (let [ch-local-treasures (chan)]

          (async/pipe ch-local-treasures ch-treasures)

          (loop []
            (if-let [report (<-area-queue *explore-reports-for-dig)]
              (dig {:total-amount (:amount report)
                    :posX         (get-in report [:area :posX])
                    :posY         (get-in report [:area :posY])}
                   ch-local-treasures)
              (Thread/sleep 1))
            (recur)))))

    (let [ch-coins (chan)
          handler (fn [{:keys [id depth]} ch]
                    
                    (postreq-cash id
                                  (fn [{:keys [body request-time]}]
                                    (go
                                      (swap! *stat update :exchange-treasure-count (fnil inc 0))
                                      (swap! *stat update (str "coint" depth) #(float (/ (+ (or % request-time) request-time) 2)))
                                      (>! ch body)
                                      (async/close! ch)))
                                  (fn [ex]
                                    ;(let [tho (slingshot.slingshot/get-thrown-object ex)]
                                     ; (println "cash= status:" (:status tho) "message:" (:message tho)))
                                    (async/close! ch))))]
      (async/pipeline-async CASH_PARALLELS
                            ch-coins
                            handler
                            ch-treasures)
      (go-loop []
        (let [coins (<! ch-coins)]
          (swap! *coins into coins)
          )
        (recur)))))

(defn main [& opts]
  (println "STEP1" STEP1
           "CASH_PARALLELS" CASH_PARALLELS
           "EXPLORE1_PARALLELS" EXPLORE1_PARALLELS
           "EXPLORE2_PARALLELS" EXPLORE2_PARALLELS
           "DIG_PARALLELS" DIG_PARALLELS)  

  (run)

  (print-stat)

  (future (loop []
            (Thread/sleep 1000000)
            (recur))))

(comment
  
  (main)

  )
