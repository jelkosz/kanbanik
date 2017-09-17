(ns org.kanbanik.statistics
  (:use clojure.data)
  (:require [clojure.string :as str]))

(defn first-timestamp [stream]
  "Takes a list of task related events and returns the timestamp - 1 for the first, if the
  timeframe contains at least one element, otherwise -1"
  (if (and
       (> (count stream) 0 )
       (contains? (first stream) :timestamp))
    (- (:timestamp (first stream)) 1)
    -1
  )
)

(defn apply-filter [filters tasks]
"
Takes a conditions object and returns the list of tasks which satisfy it.
The conditions object has the following structure:
{
:operator and/or/not
:examples [{:operator not :example {example1}} {:example {example 2}}]
}
"
  (if (empty? filters)
    tasks
    (if (:example filters)
      (filter (fn [task]
       (let [differs? (nil? (first (diff (:example filters) task)))
             not? (= (:operator filters) :not)]
         (if not? (not differs?) differs?)
         )) tasks)
      tasks)))

; a nasty hack just for playing around - will be removed
(ns-unmap *ns* 'reduce-function)

(defmulti reduce-function (fn [chunk-with-function] (:function chunk-with-function)))
 (defmethod reduce-function :merge
  [chunk-with-function]
  (apply merge (:chunk chunk-with-function)))
 (defmethod reduce-function :last
  [chunk-with-function]
  (last (:chunk chunk-with-function)))

; TODO optimize - store only the needed part in the meta
(defn progressive-count [chunk-with-function]
"
Example data
{
   :meta {entityId chunk-specific-things}
   :data 
     {
       {
         :projectId (:projectId chunk)
         :boardId (:boardId chunk)
         :workflowitem (:workflowitem chunk)
       } [1 2 3 2]
     }
  }
"

 (defn conj-to-data [prev new-val]
   (if (empty? prev)
     [(Math/max 0 new-val)] ; for adding it is 1, for deleting it is 0
     (conj prev (+ (last prev) new-val))))

  (let [
        chunk (:chunk chunk-with-function) 
        prev (:prev chunk-with-function)
        place-id {
         :projectId (:projectId chunk)
         :boardId (:boardId chunk)
         :workflowitem (:workflowitem chunk)
       }]
    
    (if (= "TaskCreated" (:eventType chunk))
      (assoc-in
       (assoc-in prev [:meta (:entityId chunk)] chunk)
       [:data place-id] (conj-to-data (get (:data prev) place-id) 1))
      (if (= "TaskDeleted" (:eventType chunk))
        (assoc-in
         (assoc-in prev [:meta (:entityId chunk)] chunk)
         [:data place-id] (conj-to-data (get (:data prev) place-id) -1))
        (if (= "TaskMoved" (:eventType chunk))
          (let [prev-place-id (assoc-in place-id [:workflowitem] (get-in prev [:meta (:entityId chunk) :workflowitem]))]
            (assoc-in
              (assoc-in
                (assoc-in 
                  prev [:meta (:entityId chunk)] chunk)
                  [:data place-id] (conj-to-data (get (:data prev) place-id) 1))
                  [:data prev-place-id] (conj-to-data (get (:data prev) prev-place-id) -1))

))))))

(defn reduce-chunk [specific-function grouped-chunks]
    ; {1 [{:timestamp 10, :entityId 1} {:timestamp 20, :entityId 1}], 2 [{:timestamp 30, :entityId 2}]}
  (map 
   (fn [grouped-chunk] (reduce-function {:function specific-function :chunk (val grouped-chunk)}))
   grouped-chunks))


(defn reduce-tasks [specific-function forward-filter grouped]
    "Takes a list of tasks grouped by timestamp and the first timestamp
  which is used as a base.
  From each chunk returns only the last task enriched by the :timestamp
  attribute which contains the time difference between the base timestamp and
  the last timestamp."
      (loop [res [] prev [] vals grouped]
          (if (= (count vals) 0)
            res
            (let [vals-with-prev-vals (concat (apply-filter forward-filter prev) (first vals))
                reduced (reduce-chunk specific-function (group-by #(:entityId %) vals-with-prev-vals))]
            (recur (conj res reduced) reduced (rest vals))))))

(defn group-by-timeframe-dense [stream timeframe]
      "Gets a vector of task related events sorted by time and groups them according to given time frame.
  Timeframe: in seconds
  Input: [event1 event2 event3 event4....]
  Output {timeframe1 [event1 event2...] timeframe2 [event3 event4...]...]}
         if the timeframe is nil, the original stream is returned
"
      (if (nil? timeframe)
        {nil stream}
        (if (= (count stream) 0)
          []
          ; the base time starts one millisecond before the first item from the stream
          (let [base-timestamp (first-timestamp stream)]
            (group-by (fn [item]
                        (Math/ceil (/
                                    (- (:timestamp item) base-timestamp) 
                                    (max timeframe 1)))
                        )
                      stream)
            )
          )))

(defn group-by-timeframe [stream timeframe]
  "Adds empty placeholder vectors to the dense group so there is a place to forward the events to."
  (if (= 0 (count stream))
    []
    (let [dense (group-by-timeframe-dense stream timeframe)]
      (if (nil? timeframe)
        (map (fn [[k v]] v) dense) ; remove the timestamp part
        (let [k (sort (keys dense))
              new-range (range (first k) (+ (last k) 1))]
          (into [] (map (fn [i]
                      (let [val (get dense i)]
                        (if (nil? val)
                          []
                          val
                          )
                        ))
                    new-range
                    )))))))

"Defines the map of functions which can be used"
(def functions
  {:cnt (fn [tasks] (count tasks))
   :pass (fn [tasks] tasks)}
)


(defn apply-function [function tasks]
  (if (and (not (nil? function)) (function functions)) 
    ((function functions) tasks)
    nil
  )
)

(defn generate-report [descriptors tasks]
"
The tasks are one timeframe output from the reduce-tasks

Example input (aka descriptor)
(def d [{:function :cnt :filter {:workflowitem-id 10} :children [{:function :cnt :filter {:workflowitem-id 10}}]}])

Example input (aka list of tasks)
(def t [{:workflowitem-id 10} {:workflowitem-id 20}])

Example output
[1 [1]]
"
  (loop [res [] desc descriptors]
    (if (= (count desc) 0)
      res
      (let [filtered-tasks (apply-filter (:filter (first desc)) tasks) function (:function (first desc))]
           (if (:children (first desc))
             (recur (conj res (apply-function function filtered-tasks) (generate-report (:children (first desc)) filtered-tasks)) (rest desc))
             (recur (conj res (apply-function function filtered-tasks)) (rest desc))
             )           
       )
    )
  )
)

(defn run-analisis [descriptor timeframe stream]
  (map #(generate-report (:result-descriptors descriptor) %)
       (reduce-tasks 
        (:reduce-function descriptor)
        (:forward-filter descriptor)
        (group-by-timeframe  stream timeframe))))
