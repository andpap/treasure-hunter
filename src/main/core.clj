(ns core
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [slingshot.slingshot :as sl :refer [throw+ try+]])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def base-url (str "http://" (or (System/getenv "ADDRESS") "localhost") ":8000"))

;(def cm (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 1000 :threads 16}))

(def STEP1 16)
(def WORD_SIZE 3488)
(def CASH_PARALLELS 2)
(def EXPLORE_PARALLELS 4)
(def DIG_PARALLELS 4)
(def MIN_AMOUNT 6)
(def LICENSES_TIMEOUT 10)
(def CONNECTION_TIMEOUT 10)
(def RPS 950)
(def *rps-counter (atom 0))

(def *reports-queue (LinkedBlockingQueue.))
(defn add-report [report]
  (.add *reports-queue report))
(defn poll-report []
  (.poll ^LinkedBlockingQueue *reports-queue))
(defn report-queue-not-full? []
  (<= (count *reports-queue) 4))

(def *reports-dig-queue (LinkedBlockingQueue.))
(defn add-report-dig [report]
  (.add *reports-dig-queue report))
(defn poll-report-dig []
  (.poll ^LinkedBlockingQueue *reports-dig-queue))

(def *treasures-queue (LinkedBlockingQueue.))
(defn add-treasure [treasure]
  (.add *treasures-queue treasure))
(defn poll-treasure []
  (.poll ^LinkedBlockingQueue *treasures-queue))

(defn getreq [path {:keys [success raise]}]
  (client/get (str base-url "/" path)
              {:as :json
               :content-type :json
               :async? true
               :accept :json}
              success
              raise))

(comment
  (clojure.pprint/pprint @*licenses)
  (getreq "licenses"
          {:success (fn [{:keys [body]}]
                      (println body))
           :raise (fn [er]
                    (println er))}))

(def *request-count (atom 0))


;(future
;  (loop []
    

;    (recur)))

(defn postreq
  [path {:keys [data success raise timeout]
         :or {timeout 1000
              raise println
              success println}}]
;  (Thread/sleep 10)
;;  (loop []
;    (when (< @*rps-counter))

;    (recur))
  (swap! *request-count inc)
  (let [future (client/post (str base-url path)
                            {:as :json
                             :content-type :json
                             :accept :json
                             :body (json/encode data)
                             :async? true
                             :oncancel (fn [& a] (raise :timeout))}
                            success
                            raise)]
;;    (try
;;      (.get future timeout java.util.concurrent.TimeUnit/MILLISECONDS)
;;      (catch java.util.concurrent.TimeoutException e
;;        (swap! *rps-counter dec)
;;        (println "cancelled" path)
;;        (.cancel future true)))
    ))

(defn postreq-explore
  [opts]
  (postreq "/explore"
           opts))

(defn postreq-dig
  [opts]
  (postreq "/dig"
           opts))

(defn postreq-license
  [opts]
  (postreq "/licenses"
           (assoc opts :timeout 2000)))

(defn postreq-cash
  [opts]
  (postreq "/cash"
           opts))

(def *licenses (ref {}))
(def *licenses-for-refresh (ref {}))
(def *coins (atom '()))
(def *stat (atom {}))
(def *cache-thread-licenses (ref {}))
(def *license-price (atom 0))
(def *phase (atom "EXPLORE"
                  :validator #{"EXPLORE" "DIG" "CASH"}))
(defn explore-phase? []
  (= "EXPLORE" @*phase))
(defn dig-phase? []
  (= "DIG" @*phase))
(defn cash-phase? []
  (= "CASH" @*phase))

(comment
  (count @*licenses)
  (count @*coins)
  (count *reports-queue)
  (count *reports-dig-queue))

(defn expired-license?
  [[id license]]
  (let [{:keys [digAllowed digUsed]} license]
    (= digAllowed digUsed)))

(defn remove-expired-licenses []
  (when-let [ids-to-delete (seq (->> @*licenses
                                     (filter expired-license?)
                                     (map #(first %))))]
    (dosync
     (alter *licenses (partial apply dissoc) ids-to-delete))))

(defn get-license []
  (remove-expired-licenses)
  (let [thread-name (.getName (Thread/currentThread))]
    (dosync
     (alter *cache-thread-licenses dissoc thread-name)
     (let [busy-license-ids (into #{} (vals @*cache-thread-licenses))
           unbusy-license-id   (->> @*licenses
                                    (filter (fn [[k v]] (not (busy-license-ids k))))
                                    (first)
                                    (second))]
       (alter *cache-thread-licenses assoc thread-name unbusy-license-id)
       unbusy-license-id))))

(defn buy-license-promise
  ([]
   (buy-license-promise []))
  ([data]
   (let [result (promise)]     
     (postreq-license {:data data
                       :success (fn [{:keys [status body request-time]}]
;                                  (println "111")
                                  (deliver result (assoc body :request-time request-time)))
                       :raise (fn [ex]
;                                (println ex)
                                (let [tho (slingshot.slingshot/get-thrown-object ex)
                                      status (:status tho)]
                                  (deliver result nil)))})
     result)))

(comment
  @(buy-license-promise [])

  )

(defn update-average-time [key time]
;  (println key "--" time)
;  (let [key_count (str key "count")
;        new-count ((swap! *stat update key_count (fnil inc 0)) key_count)
;        current-average-time (get @*stat key 0)
;        new-average-time (float (/ (+ time (* current-average-time (dec new-count))) new-count))]
;    (swap! *stat assoc key new-average-time)    
;    )
  )

(defn dig [{:keys [total-amount posX posY]} *monitor]
  (loop [depth             1
         remaining-amount total-amount]
    (when (and (< depth 11) (> remaining-amount 0))

      (if-let [license (get-license)]
        (if (and (:free license) (> depth 6))
          nil
          
        (let [result  (promise)
              license-id (:id license)]
          (swap! *monitor inc)
          (postreq-dig {:data {:licenseID license-id
                               :posX posX
                               :posY posY
                               :depth depth}
                        :success (fn [{:keys [status body request-time]}]
                                   (deliver result
                                            {:status status
                                             :request-time request-time
                                             :body body}))
                        :raise (fn [ex]
                                 (if (= :timeout ex)
                                   (deliver result {:status :timeout})
                                   (let [tho (slingshot.slingshot/get-thrown-object ex)]
                                     (deliver result tho))))})
          (let [{:keys [status body request-time]} @result]
            (swap! *monitor dec)
            (cond
              (nil? status) nil
              (= :timeout status) (dosync
                                   (alter *licenses dissoc license-id)
                                   (alter *licenses-for-refresh assoc license-id license))
              (= 200 status) (do
                               (update-average-time (format "dig[%s]" depth) request-time)
                               (dosync
                                (alter *licenses update-in [license-id :digUsed] inc))

                               (doseq [treasure-id body]
                                 (add-treasure {:id treasure-id
                                                :depth depth}))
                               (recur (inc depth) (- remaining-amount (count body))))
              (= 404 status) (do
                               (update-average-time (format "dig[%s]" depth) request-time)
                               (dosync
                                (alter *licenses update-in [license-id :digUsed] inc))

                               (recur (inc depth) remaining-amount))
              (= 403 status) (do
                               ;(recur depth remaining-amount)
                               )
              :default        (do
                                (println "repead dig status " status)
                                (recur depth remaining-amount))))))
        (recur depth remaining-amount)))))

(defn print-stat []
  (future
    (loop []
      (Thread/sleep (* 1000 10))
      (println "licenses count:" (count @*licenses))
      (println "licenses for recover:" (count @*licenses-for-refresh))
      (println "reports count:" (count *reports-queue))
      (println "treasures count:" (count *treasures-queue))
      (println "digreports count:" (count *reports-dig-queue))
      (println "licenses price:" @*license-price)
;      (clojure.pprint/pprint @*licenses)
      (clojure.pprint/pprint @*stat)
      (recur))))

(defn run []
  (future
    (loop []
      (let [dig-queue-length (count *reports-dig-queue)]
        (cond
          (explore-phase?)
          (when (> dig-queue-length 100)
            (swap! *phase (constantly "DIG")))
          (dig-phase?)
          (when (<= dig-queue-length 1)
            (swap! *phase (constantly "EXPLORE")))

         ; (dig-phase?)
         ; (when (zero? dig-queue-length)
         ;   (swap! *phase (constantly "CASH")))
         ; (cash-phase?)
         ; (when (zero? (count *treasures-queue))
         ;   (swap! *phase (constantly "EXPLORE")))
          ))
      (Thread/sleep 500)
      (recur)))

  (future
    (loop []
      (Thread/sleep 1000)
      (when (> (count @*licenses-for-refresh) 0)
        (let [p (promise)]
          (getreq "licenses"
                  {:success (fn [{:keys [body]}]
                              (dosync
                               (let [licenses-tmp @*licenses-for-refresh]
                                 (ref-set *licenses-for-refresh {})
                                 (doseq [license  body]
                                   (when (get licenses-tmp (:id license))
                                     (update-in licenses-tmp
                                                [(:id license) :digUsed]
                                                (constantly (:digUsed license)))))
                                 (alter *licenses merge licenses-tmp)
                                 (deliver p true))))
                   :raise (fn [er]
                            (println "get-licenses:" (.getMessage er))
                            (deliver p true))})
          @p))
      (recur)))

  (future
    (let [*count (atom 0)]
      (loop []
        (remove-expired-licenses)
        (let [licenses-count (count @*licenses)]
          (when (and
                 (< @*license-price 25)
                 (< licenses-count 5)
                 (> @*count 10))
            (reset! *count 0)
            (swap! *license-price inc))
          (when (< licenses-count 10)
            (let [price @*license-price
                  coins (take price @*coins)
                  _     (swap! *coins #(drop price %))]
              (when-some [license @(buy-license-promise coins)]
                (let [coins-count (count coins)]
                  (dosync
                   (alter *licenses assoc (:id license) (assoc license :free (zero? coins-count))))
                  (swap! *stat update "sum license price:" (fnil + 0) coins-count)
                  (when (= price coins-count)
                    (swap! *count inc)))

                (Thread/sleep LICENSES_TIMEOUT)))))
        (recur))))

  (future
    (let [areas (for [x (range 0 WORD_SIZE STEP1)
                      y (range 0 WORD_SIZE STEP1)]
                  {:posX x :posY y :sizeX STEP1 :sizeY STEP1})
          start (fn [area]
                  (postreq-explore {:data area
                                    :success (fn [{:keys [body]}]
                                               (when (>= (:amount body) MIN_AMOUNT)
                                                 (add-report body)))}))]
      (loop [areas areas]
        (if (and (explore-phase?)
                 (report-queue-not-full?))
          (do
            (start (first areas))
            (recur (rest areas)))
          (do
            (Thread/sleep 10)
            (recur areas))))))

  (dotimes [n EXPLORE_PARALLELS]
    (future
      (let [*monitor (atom 0)]
        (letfn [(area-1x1?
                  [{:keys [sizeX sizeY]}]
                  (= 1 sizeX sizeY))
                (process [{:keys [amount area]} end-callback]
                  (let [{:keys [posX posY sizeX sizeY]} area
                        areas (if (= sizeX sizeY)
                                [{:posX posX :posY posY :sizeX (/ sizeX 2) :sizeY sizeY}
                                 {:posX (+ posX (/ sizeX 2)) :posY posY :sizeX (/ sizeX 2) :sizeY sizeY}]
                                [{:posX posX :posY posY :sizeX sizeX :sizeY (/ sizeY 2)}
                                 {:posX posX :posY (+ posY (/ sizeY 2)) :sizeX sizeX :sizeY (/ sizeY 2)}])]
                    (swap! *monitor inc)
                    (postreq-explore {:data (first areas)
                                      :success (fn [{:keys [body]}]
                                                 (swap! *monitor dec)
                                                 (let [current-amount (:amount body)
                                                       remaining-amount (- amount current-amount)]
                                                   (when (> current-amount 0)
                                                     (end-callback body))
                                                   (when (> remaining-amount 0)
                                                     (end-callback {:amount remaining-amount
                                                                    :area (second areas)}))))
                                      :raise (fn [er]
                                               (swap! *monitor dec))})))
                (end-callback [resp]
                  (if (area-1x1? (:area resp))
                    (add-report-dig resp)
                    (process resp end-callback)))]
          (loop []
            (when (zero? @*monitor)
              (when-let [report (poll-report)]
                (process report end-callback)))
            (Thread/sleep 1)
            (recur))))))

  (dotimes [n DIG_PARALLELS]
    (future
      (let [*monitor (atom 0)]
        (loop []
          (if (and (dig-phase?)
                   (zero? @*monitor))
            (if-let [report (poll-report-dig)]
              (dig {:total-amount (:amount report)
                    :posX         (get-in report [:area :posX])
                    :posY         (get-in report [:area :posY])}
                   *monitor))
            (Thread/sleep 10))
          (recur)))))

  (dotimes [n CASH_PARALLELS]
    (future
      (let [*monitor (atom 0)]
        (loop []
          (if (zero? @*monitor)
            (when-let [{:keys [id depth]} (poll-treasure)]
              (when (or (> depth 3) (< (count @*coins) 101))
                (swap! *monitor inc)
                (postreq-cash {:data id
                               :success (fn [{:keys [body request-time]}]
                                          (swap! *monitor dec)
                                          (update-average-time (format "cash[%s]" depth) request-time)
                                          (swap! *coins into body))
                               :raise (fn [ex]
                                        (swap! *monitor dec))}))

              ))
          (recur))))))

(defn main [& opts]
  (println "STEP" STEP1
           "CASH_PARALLELS" CASH_PARALLELS
           "DIG_PARALLELS" DIG_PARALLELS
           "EXPLORE_PARALLELS" EXPLORE_PARALLELS
           "MIN_AMOUNT" MIN_AMOUNT
           "LICENSES_TIMEOUT" LICENSES_TIMEOUT
           "CONNECTION_TIMEOUT" CONNECTION_TIMEOUT)

  (run)

  (print-stat)

  (future (loop []
            (Thread/sleep 1000000)
            (recur))))

(comment

  (main)

  )
