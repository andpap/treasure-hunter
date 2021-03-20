(ns core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [slingshot.slingshot :as sl :refer [throw+ try+]]
            [clojure.core.async :as async :refer [chan go-loop <! >! thread timeout go]])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def EXPLORE_BUFFER_SIZE 20)
(def WORD_SIZE 3500)
(def EXPLORE_STEP 10)
(def EXPLORE_SPEP_SMALL 2)
(def DIG_BUFFER_SIZE 2)
(def MIN_AMOUNT 3)
(def DIG_TIMEOUT_EMITTER 1000)

(def base-url (str "http://" (or (System/getenv "ADDRESS") "localhost") ":8000"))

(def begin-time (System/currentTimeMillis))

(defn post-req [path data]
  (let [resp (client/post (str base-url "/" path)
                          {:as :json
                           :content-type :json
                           :accept :json
                           :body (json/encode data)})
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
   (post-req "licenses" data)))

(defn dig [data]
  (try+
   (post-req "dig" data)
   (catch [:status 404] _
       nil)))

(defn explore [data]
  (post-req "explore" data))

(defn cash [data]
  (post-req "cash" data))

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

(def exit-ch1 (chan))
(def exit-ch2 (chan))
(def exit-ch3 (chan))
(def exit-ch4 (chan))
(def exit-ch5 (chan))
(def exit-ch6 (chan))
(def exit-ch7 (chan))

(def *word-map (atom {}))
(def *licenses (atom {}))
; {amount [areas...]}
(def *amount-big-areas-map (atom (sorted-map-by >)))
(def *amount-small-areas-map (atom (sorted-map-by >)))
(def *amount-single-areas-map (atom (sorted-map-by >)))
(def ch-coord1 (chan EXPLORE_BUFFER_SIZE))
(def ch-coord2 (chan EXPLORE_BUFFER_SIZE))
(def ch-coord3 (chan EXPLORE_BUFFER_SIZE))
(def *ex ( atom []))
(def *stat (atom {}))

(defstruct area :posX :posY :sizeX :sizeY)
(defstruct report :area :amount)

(defn save-exception
  [e tag]
 (swap! *ex conj (str tag ":" (.getMessage e))))

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
          (>! ch r))))
;    (swap! *stat assoc (str "step" step ":") (- (System/currentTimeMillis) begin-time))
    ))

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
  (swap! *amount-areas-map
         (fn [m amount area]
           (let [queue (or (m amount) (java.util.concurrent.LinkedBlockingQueue.))
                 _     (.add queue area)
                 m     (assoc m amount queue)]
             m))         
         (:amount report)
         report))

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
  (let [*wait-monitor (atom 0)]
    (go-loop []
      (async/alt!
        exit-ch5
        ([_] (println "exit-ch5"))
        (timeout 100)
        ([_]
         (remove-expired-licenses)
         (swap! *wait-monitor dec-monitor)
         (when (and (monitor<=0? *wait-monitor)
                    (< (count @*licenses) 3))
           (try
             (let [license (buy-license)]
               (swap! *licenses assoc (:id license) license))
             (catch Exception e
               (swap! *wait-monitor + 10)
;               (save-exception e "license")
               )))
         (recur)))))

  (go-loop []    
    (async/alt!
      exit-ch1
      ([_] (println "exit-ch1"))
      ch-coord1
      ([{:keys [area]}]
       (try
         (let [report (explore area)]
           (when (>= (:amount report) MIN_AMOUNT)
             (->area-queue *amount-big-areas-map report)))
         (catch Exception e
           (save-exception e "ch-coord1")))
       (recur))))

  (let [*current-buffer-size (atom 0)
        max-buffer-step-size 25]
    (thread
      (loop []
        (async/alt!!
          exit-ch3
          ([_] (println "exit-ch3"))
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
      (async/alt!
        exit-ch4
        ([_] (println "exit-ch4"))
        ch-coord2
        ([{:keys [area]}]
         (try
           (let [report (explore area)]
             (when (> (:amount report) 0)
               (->area-queue *amount-small-areas-map report)))
           (catch Exception e
             (save-exception e "ch-coord2")))
           (swap! *current-buffer-size dec-monitor)
         (recur)))))

  (let [*current-buffer-size (atom 0)
        max-buffer-step-size 4]
    (thread
      (loop []
        (async/alt!!
          exit-ch6
          ([_] (println "exit-ch6"))
          (timeout 10)
          ([_]
           (when (< @*current-buffer-size max-buffer-step-size)
             (when-some [report (<-area-queue *amount-small-areas-map)]
               (swap! *current-buffer-size + max-buffer-step-size)
               (send-coords ch-coord3
                            report
                            1)))
           (recur)))))

    (go-loop []
      (async/alt!
        exit-ch7
        ([_] (println "exit-ch7"))
        ch-coord3
        ([{:keys [area]}]
         (try
           (let [report (explore area)]
             (when (> (:amount report) 0)               
               (->area-queue *amount-single-areas-map report)))
           (catch Exception e
             (save-exception e "ch-coord2")))
           (swap! *current-buffer-size dec-monitor)
         (recur)))))

                                        ;gets amount small area and dig while amount 0 or level 10

  (defn dig-dig
    [{:keys [amount area]}]
    (loop [depth  1
           amount amount]
      (if (or (== 0 amount)
              (> depth 10))
        nil
        (let [id (get-licence-id)]
          (if id
            (let [resp     (dig {:licenseID id
                                 :posX (:posX area)
                                 :posY (:posY area)
                                 :depth depth})
                  _        (swap! *licenses update-in [id :digUsed] inc)
                  _        (println resp)
                  remain   (if (nil? resp)
                             amount
                             (dec amount))]
              (recur (inc depth)
                     remain))
            (recur depth
                   amount))))))

      
;      (let [license-id (get-licence-id)]
;        (if license-id
;          (let [resp         (dig {:licenseID license-id                                   
;                                   :posX (:posX area)
;                                   :posY (:posY area)
;                                   :depth depth})
 ;               remaining-amount (if (nil? resp) a (dec a))]
;            (swap! *licenses update-in [license-id :digUsed] inc)
;            (println "dig>>>" resp)
;            (when (and (< depth 10) (> remaining-amount 0))
;              (recur (inc depth) remaining-amount))))
;        (recur depth a))))

  (thread
    (loop []
      (when-some [report (<-area-queue *amount-single-areas-map)]
        ;(println "explore>" (explore (:area report)))
        ;(println report)
        (dig-dig report))
      (Thread/sleep 5000)
      (recur)))
  

  (go-loop []
    (async/alt!
      exit-ch2
      ([_] (println "exit-ch2"))
      (timeout 20000)
      ([_]
       (doseq [line (partition-all 6 @*stat)]
         (println line))
       (when (seq @*ex)
         (println @*ex))
;       (println (- (System/currentTimeMillis) begin-time))
       (recur)))))
  

(defn main [& opts]
  (println  "STEP:" EXPLORE_STEP
            "EXPLORE_BSz" EXPLORE_BUFFER_SIZE
            "DIG_BSz" DIG_BUFFER_SIZE
            "MIN_AMOUNT" MIN_AMOUNT
            "DIG_TIMEOUT" DIG_TIMEOUT_EMITTER)

  (start-go-loops)
  
  (let [start-report (struct report
                             (struct area 0 0 WORD_SIZE WORD_SIZE))]
        (send-coords ch-coord1
                     start-report
                     EXPLORE_STEP))

  (future (loop []
             (Thread/sleep 1000000)
             (recur))))

(defn stop []
  (go (>! exit-ch1 1))
  (go (>! exit-ch2 1))
  (go (>! exit-ch3 1))
  (go (>! exit-ch4 1))
  (go (>! exit-ch5 1))
  (go (>! exit-ch6 1))
  (go (>! exit-ch7 1)))

 
(comment
  (main)

  (stop)

  (let [r (async/alt!!
            (timeout 1000) ([_] 1)
            (timeout 2000) ([_] 2))]
    (println r))
  
)
