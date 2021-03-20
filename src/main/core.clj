(ns core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [slingshot.slingshot :as sl :refer [throw+ try+]]
            [clojure.core.async :as async :refer [chan go-loop <! >! thread timeout go]])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def EXPLORE_BUFFER_SIZE 4)
(def WORD_SIZE 3500)
(def EXPLORE_STEP 10)
(def EXPLORE_SPEP_SMALL 2)
(def DIG_THREADS 2)
(def MIN_AMOUNT 3)
(def LICENSE_TIMEOUT 50)

(def base-url (str "http://" (or (System/getenv "ADDRESS") "localhost") ":8000"))

(def begin-time (System/currentTimeMillis))

(defn post-req [path data]
  (let [resp (client/post (str base-url "/" path)
                          {:as :json
                           :content-type :json
                           :accept :json
                           :body (json/encode data)
                           :retry-handler (fn [ex try-count http-context]
                                            (if (> try-count 2) false true))})
        result (:body resp)]
    result))

(defn get-req [path]
  (let [resp (client/get (str base-url "/" path)
                         {:as :json
                          :content-type :json
                          :accept :json})
        result (:body resp)]
    result))

(defn buy-license
  ([]
   (buy-license []))
  ([data]
   (try+
    (post-req "licenses" data)
    (catch [:status 504] _
      nil)
    (catch [:status 503] _
      nil)
    (catch [:status 502] _
      nil)
    (catch Object _
      (println "license:" &throw-context)))))

(defn dig [data]
  (try+
   (post-req "dig" data)
   (catch [:status 404] _
     nil)
   (catch Object _
     (println "dig:" &throw-context))))

(defn explore [data]
  (post-req "explore" data))

(defn cash [data]
  (try+
   (post-req "cash" data)
   (catch [:status 503] _
     nil
     )
   (catch Object _
     (println "cash:" &throw-context))))

(defn get-licenses []
  (get-req "licenses"))

(comment
  (println
   (get-licenses)
   )
  (dotimes [n 12]
    (try+
     (buy-license)
     (catch [:status 409] {:keys [request-time headers body]}
       (println body))
     (catch Object _
       (println &throw-context))))

  (slingshot.slingshot/try+)

  )

(def *licenses (atom {}))
(def *amount-big-areas-map (atom (sorted-map-by >)))
(def *amount-small-areas-map (atom (sorted-map-by >)))
(def *amount-single-areas-map (atom (sorted-map-by >)))
(def ch-coord1 (chan EXPLORE_BUFFER_SIZE))
(def ch-coord2 (chan EXPLORE_BUFFER_SIZE))
(def *treasures (atom {}))
(def *ex ( atom {}))
(def *stat (atom {}))

(defstruct area :posX :posY :sizeX :sizeY)
(defstruct report :area :amount)

(defn save-exception
  [e tag]
  (swap! *ex
         assoc
         (str tag ":" (.getMessage e))
         (fnil inc 0)))

(defn expired-license?
  [[id license]]
  (let [{:keys [digAllowed digUsed]} license]
    (= digAllowed digUsed)))

(defn send-coords
  [ch rep step]
  (go
    (let [{:keys [posX posY sizeX sizeY]} (:area rep)]
      (doseq [x (range posX (+ posX sizeX) step)
              y (range posY (+ posY sizeY) step)]
        (let [r (struct report (struct area x y step step))]
          (>! ch r))))))

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

(defn remove-expired-licenses []
  (when-let [ids-to-delete (seq (->> @*licenses
                                     (filter expired-license?)
                                     (map #(first %))))]
    (swap! *licenses (partial apply dissoc) ids-to-delete)))


(defn dec-monitor
  [v]
  (if (> v 0)
    (dec v)
    0))

(defn monitor<=0? [*m]
  (<= @*m 0))


(defn get-licence-id []
  (remove-expired-licenses)
  (->> @*licenses
       (first)
       (first))
  )

(defn start-go-loops []

  (thread
    (loop []
      (remove-expired-licenses)
      (when (< (count @*licenses) 10)
        (when-some [license (buy-license)]
          (swap! *stat update :lic-count (fnil inc 0))
          (swap! *licenses assoc (:id license) license))
        (Thread/sleep LICENSE_TIMEOUT))
      (recur)))

  (go-loop []
    (let [{:keys [area]} (<! ch-coord1)]
     (try
       (let [report (explore area)]
         (when (>= (:amount report) MIN_AMOUNT)
           (->area-queue *amount-big-areas-map report)))
       (catch Exception e
         (save-exception e "ch-coord1")))
     (recur)))

  (let [*current-buffer-size (atom 0)
        max-buffer-step-size 25]
    (thread
      (loop []
        (async/alt!!
          (timeout 10)
          ([_]
           (when (< @*current-buffer-size max-buffer-step-size)
             (when-some [report (<-area-queue *amount-big-areas-map)]
               (swap! *current-buffer-size + max-buffer-step-size)
               (send-coords ch-coord2
                            report
                            2)))
           (recur)))))

    (go-loop []
      (let [{:keys [area]} (<! ch-coord2)]
         (try
           (let [report (explore area)]
             (->area-queue *amount-small-areas-map report))
           (catch Exception e
             (save-exception e "ch-coord2")))
           (swap! *current-buffer-size dec-monitor)
           (recur))))

  (go-loop []
    (when-some [{:keys [amount area]} (<-area-queue *amount-small-areas-map)]
      (let [{:keys [posX posY sizeX sizeY]} area
            *total-amount (atom amount)]
        (doseq [x (range posX (+ posX sizeX))
                y (range posY (+ posY sizeY))]
          (when (> @*total-amount 0)
            (when-some [report (explore {:posX x :posY y :sizeX 1 :sizeY 1})]
              (->area-queue *amount-single-areas-map report)
              (swap! *total-amount - (:amount report))))
          )))
    (recur))

  (defn dig-dig
    [{:keys [amount area]}]
    (loop [depth  1
           amount amount]
      (if (or (== 0 amount)
              (> depth 10))
        nil        
        (if-let [id (get-licence-id)]
          (let [resp     (dig {:licenseID id
                               :posX (:posX area)
                               :posY (:posY area)
                               :depth depth})
                _        (swap! *licenses update-in [id :digUsed] inc)
;                _        (swap! *stat update :dig-count (fnil inc 0))
;                _        (println resp)
                remain   (if (nil? resp)
                           amount
                           (do
                             (swap! *treasures (partial apply assoc) (interleave resp (repeat 1)))
                             (swap! *stat update :dig-count (fnil inc 0))
                             (dec amount)))]
            (recur (inc depth)
                   remain))
          (recur depth
                 amount)))))
  
  (go-loop []
    (when-some [report (<-area-queue *amount-single-areas-map)]
      (dig-dig report))
    (recur))

  (thread
    (loop []
      (when-some [tr (first @*treasures)]
;        (println tr)
        (let [[id _] tr
              _    (swap! *treasures dissoc id)
              resp (cash id)]
          (Thread/sleep 100)
          ))        
      (recur)))


  (go-loop []
    (async/alt!
      (timeout 10000)
      ([_]
       (doseq [line (partition-all 6 @*stat)]
         (println line))
       (when (seq @*ex)
         (println @*ex))
       (println "threasures:" (count @*treasures))
       (println "licenses:" (count @*licenses))
       (println (- (System/currentTimeMillis) begin-time) "ms")
       (recur)))))
  


(defn main [& opts]
  (println  "STEP:" EXPLORE_STEP
            "EXPLORE_BSz" EXPLORE_BUFFER_SIZE
            "DIG_THs" DIG_THREADS
            "MIN_AMOUNT" MIN_AMOUNT
            "LICENSETIMEOUT" LICENSE_TIMEOUT)

  (start-go-loops)
  
  (let [start-report (struct report
                             (struct area 0 0 WORD_SIZE WORD_SIZE))]
        (send-coords ch-coord1
                     start-report
                     EXPLORE_STEP))

  (future (loop []
             (Thread/sleep 1000000)
             (recur))))

(comment
  (main)


  (let [r (async/alt!!
            (timeout 1000) ([_] 1)
            (timeout 2000) ([_] 2))]
    (println r))
  
)
