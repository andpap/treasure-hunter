(ns core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [slingshot.slingshot :as sl :refer [throw+ try+]]
            [clojure.core.async :as async :refer [chan go-loop <! >! thread timeout go]])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def base-url (str "http://" (or (System/getenv "ADDRESS") "localhost") ":8000"))

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
                :socket-timeout 1000
                :connection-timeout 1000
                :body (json/encode data)
                :async? true}
               success
               raise))

(defn postreq-explore [data success raise]
  (postreq "/explore" data success raise))

(defn postreq-dig [data success raise]
  (postreq "/dig" data success raise))

(defn postreq-license [data success raise]
  (postreq "/licenses" data success raise))

(defn postreq-cash [data success raise]
  (postreq "/cash" data success raise))

(def *licenses (atom {}))

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
  (->> @*licenses
       (first)
       (first)))

(defn buy-license-promise
  ([]
   (buy-license-promise []))
  ([data]
   (let [result (promise)]
     (postreq-license data
                      (fn [{:keys [status body]}]
                        (deliver result body))
                      (fn [ex]
                        (let [tho (slingshot.slingshot/get-thrown-object ex)
                              status (:status tho)]
                          (deliver result nil))))
     result)))

(comment
  (getreq "licenses")
  (future
    (println (buy-license)))
  (future
    (println (dig {:total-amount 10 :posX 100 :posY 10})))
  (postreq-dig {:licenseID 1
                :posX 0
                :posY 0
                :depth 5}
               (fn [resp]
                 (println "success:" resp))
               (fn [ex]
                 (println "ex:" ex))))

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
                       (fn [{:keys [status body]}]
                         (deliver result {:status status
                                          :body body}))
                       (fn [ex]
                         (let [tho (slingshot.slingshot/get-thrown-object ex)]
                           (deliver result tho))))
          (let [{:keys [status body]} @result]
            (cond
              (== 200 status) (do
                                (swap! *licenses update-in [license-id :digUsed] inc)
                                (doseq [treasure-id body]
                                  (async/put! ch treasure-id))
                                (recur (inc depth) (- remaining-amount (count body))))
              (== 404 status) (do
                                (swap! *licenses update-in [license-id :digUsed] inc)
                                (recur (inc depth) remaining-amount))
              :default        (do
                                (println "repead dig status " status)
                                (recur depth remaining-amount)))))
        (recur depth remaining-amount)))))

(defn run []
  (let [ch-areas (chan 1)
        ch-reports (chan 1)
        ch-reports-finish (chan 1)
        ch-treasures (chan)
        *explore-reports-map (atom (sorted-map-by >))
        *explore-reports-for-dig (atom (sorted-map-by >))]

    (thread
      (loop []
        (remove-expired-licenses)
        (when (< (count @*licenses) 10)
          (when-some [license @(buy-license-promise)]
            (swap! *licenses assoc (:id license) license)
            (Thread/sleep 60))
          (Thread/sleep 1))
        (recur)))

    (async/pipeline-async 4
                          ch-reports
                          (fn [area ch]
                            (postreq-explore area
                                             (fn [{:keys [body]}]
                                               (go
                                                 (when (> (:amount body) 0)
                                                   (>! ch body))
                                                 (async/close! ch)))
                                             (fn [ex]
                                               (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                                 (println "explore= status:" (:status tho) "message:" (:message tho)))
                                               (async/close! ch))))
                          ch-areas)

    (go
      (doseq [x (range 0 3500 4)
              y (range 0 3500 4)]
        (>! ch-areas {:posX x :posY y :sizeX 4 :sizeY 4})))

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
              ch-areas              (chan 2)
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
                                                           (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                                             (println "explore1= status:" (:status tho) "message:" (:message tho)))
                                                           (async/close! ch)))))]
          (async/pipeline-async 2 ch-reports-finish handler ch-areas)

          (doseq [x (range startX (+ startX (:sizeX area)) 1)
                  y (range startY (+ startY (:sizeY area)) 1)]
            (>! ch-areas {:posX x :posY y :sizeX 1 :sizeY 1}))
          (recur))

        (do
          (<! (timeout 10))
          (recur))))

    (go-loop []
      (if-let [report (<! ch-reports-finish)]
        (->area-queue *explore-reports-for-dig report))
      (recur))
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
          (recur))))

;    (go-loop []
;      (let [tr (<! ch-treasures)]
;        (println "new:" tr))
;      (recur))

    (let [ch-coins (chan)
          handler (fn [v ch]
                    (postreq-cash v
                                  (fn [{:keys [body]}]
                                    (go
                                      (>! ch body)
                                      (async/close! ch)))
                                  (fn [ex]
                                    (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                      (println "cash= status:" (:status tho) "message:" (:message tho)))
                                    (async/close! ch))))]
      (async/pipeline-async 2
                            ch-coins
                            handler
                            ch-treasures)
      (go-loop []
        (let [coins (<! ch-coins)]
         (println "coins:" coins)
          )
        (recur)))))

(defn main [& opts]

  (run)

  (future (loop []
            (Thread/sleep 1000000)
            (recur))))

(comment
  (main))
