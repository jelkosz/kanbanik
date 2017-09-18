(ns org.kanbanik.statistics-test
  (:use clojure.test)
  (:use org.kanbanik.statistics))

; hack needed to make sure the reduce-function will not be mapped multiple times
(ns-unmap *ns* 'reduce-function)

(testing "Statistics"
  (testing "first-timestamp"
    (is (= -1 (first-timestamp [])))
    (is (= -1 (first-timestamp [{:does-not-have-timestamp 10}])))
    (is (= 9 (first-timestamp [{:timestamp 10}])))
    (is (= 9 (first-timestamp [{:timestamp 10} {:timestamp 12}])))
    )

  (testing "reducer-progressive-count"
    (let [
          i1p1b1w1C {:eventType "TaskCreated" :projectId 1 :boardId 1 :workflowitem 1 :entityId 1}
          i2p1b1w1C {:eventType "TaskCreated" :projectId 1 :boardId 1 :workflowitem 1 :entityId 2}
          i2p1b1w2M {:eventType "TaskMoved" :projectId 1 :boardId 1 :workflowitem 2 :entityId 2}
          i4p1b1w2M {:eventType "TaskMoved" :projectId 1 :boardId 1 :workflowitem 2 :entityId 4}
          i1p1b1w1D {:eventType "TaskDeleted" :projectId 1 :boardId 1 :workflowitem 1 :entityId 1}
          i3p1b1w2D {:eventType "TaskDeleted" :projectId 1 :boardId 1 :workflowitem 2 :entityId 3}]
      
      (let [first-call (progressive-count {:function nil :chunk i1p1b1w1C :prev {}})]
        (is (= {1 i1p1b1w1C} (:meta first-call)))
        (is (= [1] (get (:data first-call) {:projectId 1 :boardId 1 :workflowitem 1})))
        (let [second-call (progressive-count {:function nil :chunk i2p1b1w1C :prev first-call})]
          (is (= {1 i1p1b1w1C 2 i2p1b1w1C} (:meta second-call)))
          (is (= [1 2] (get (:data second-call) {:projectId 1 :boardId 1 :workflowitem 1})))
          
          (let [third-call (progressive-count {:function nil :chunk i1p1b1w1D :prev second-call})]
            (is (= {1 i1p1b1w1D 2 i2p1b1w1C} (:meta third-call)))
            (is (= [1 2 1] (get (:data third-call) {:projectId 1 :boardId 1 :workflowitem 1})))

            (let [fourth-call (progressive-count {:function nil :chunk i2p1b1w2M :prev third-call})]
              (is (= {1 i1p1b1w1D 2 i2p1b1w2M} (:meta fourth-call)))
              (is (= [1 2 1 0] (get (:data fourth-call) {:projectId 1 :boardId 1 :workflowitem 1})))
              (is (= [1] (get (:data fourth-call) {:projectId 1 :boardId 1 :workflowitem 2})))
              ; delete not existing item (e.g. the data did not start at the beginning
              (let [fifth-call (progressive-count {:function nil :chunk i3p1b1w2D :prev fourth-call})]
                (is (= {1 i1p1b1w1D 2 i2p1b1w2M} (:meta fifth-call)))
                ; move something which has not been there
                (let [sixth-call (progressive-count {:function nil :chunk i4p1b1w2M :prev fifth-call})]
                  (is (= {1 i1p1b1w1D 2 i2p1b1w2M 4 i4p1b1w2M} (:meta sixth-call)))
                  (is (= [1 2] (get (:data sixth-call) {:projectId 1 :boardId 1 :workflowitem 2})))
                  (is (= 2 (count (:data sixth-call))))
              ))))))))

  (testing "reduce-tasks"
     (let [
          id1t10 {:timestamp 10 :entityId 1}
          id1t20 {:timestamp 20 :entityId 1}
          id2t30 {:timestamp 30 :entityId 2}
          id2t40 {:timestamp 40 :entityId 2}
          id3t10 {:timestamp 10 :entityId 3}
         ]
; one chunk, one field in it
      (is (= 10 (:timestamp (first (first (reduce-tasks :last nil [[id1t10]]))))))
; one chunk, two field with same id in it
      (is (= 20 (:timestamp (first (first (reduce-tasks :last nil [[id1t10 id1t20]]))))))
; one chunk, 3 fields with 2 differend ids in it
      (let [two-in-chunk (reduce-tasks :last nil [[id1t10 id1t20 id2t30]])]
        (is (and
             (= 20 (:timestamp (first (first two-in-chunk))))
             (= 30 (:timestamp (second (first two-in-chunk))))
             ))
        )

; two chunks and 2 differnet ids in both
      (let [two-chunks (reduce-tasks :last nil [[id1t10 id1t20 id2t30] [id2t30 id1t20 id1t10]])]
        (is (and
; test first chunk
             (= 20 (:timestamp (first (first two-chunks))))
             (= 30 (:timestamp (second (first two-chunks))))
; test second chunk
             (= 30 (:timestamp (second (second two-chunks))))
             (= 10 (:timestamp (first (second two-chunks))))
             ))
        )

; test if the filter filters something out
      (let [two-chunks (reduce-tasks :last {:example {:entityId 1}} [[id1t10 id3t10 id1t20 id2t30] [id2t40]])]
        (is (and
; test first chunk
             (= 20 (:timestamp (first (first two-chunks))))
             (= 10 (:timestamp (second (first two-chunks))))
             (= 30 (:timestamp (last (first two-chunks))))
; test second chunk
             (= 20 (:timestamp (first (second two-chunks))))
             (= 40 (:timestamp (second (second two-chunks))))
             ))
        
      ))
  )

  (testing "group-by-timeframe"
    (let [
          id1t10 {:timestamp 10 :entityId 1}
          id1t20 {:timestamp 20 :entityId 1}
          id2t30 {:timestamp 30 :entityId 2}
          id2t80 {:timestamp 80 :entityId 2}
          id1tl1 {:timestamp 1501918219733 :entityId 1}
          id2tl2 {:timestamp 1501918222287 :entityId 2}
          id3tl3 {:timestamp 1501918225608 :entityId 3}
          id4tl4 {:timestamp 1501918229303 :entityId 4}
         ]
      (is (= 0 (count (group-by-timeframe [] 1))))
      (is (= 3 (count (group-by-timeframe [id1t10 id1t20 id2t30] 10))))
      (is (= 21 (count (group-by-timeframe [id1t10 id1t20 id2t30] 1))))
      (is (= 21 (count (group-by-timeframe [id1t10 id1t20 id2t30] 0))))
      (is (= 1 (count (group-by-timeframe [id1t10 id1t20 id2t30] 100))))
      (is (= 8 (count (group-by-timeframe [id1t10 id1t20 id2t30 id2t80] 10))))
      (is (= 96 (count (group-by-timeframe [id1tl1 id2tl2 id3tl3 id4tl4] 100))))

      (testing "apply-filter"
        (let [
              w1 {:workflowitem-id 1}
              w2 {:workflowitem-id 2}
              w3 {:workflowitem-id 3}
              w4 {:workflowitem-id 4}
              ]
          (is (= 0 (count (apply-filter {:examples []} []))))
          (is (= 1 (count (apply-filter {:example w1} [w2 w1]))))
          (is (= 2 (count (apply-filter {:example {}} [w2 w1]))))
          (is (= 2 (count (apply-filter {:example {}} [{} w1]))))
          (is (= 2 (count (apply-filter {} [{} w1]))))
          (is (= 2 (count (apply-filter {:operator :not :example w4} [w2 w1]))))
          )
        ))

    (testing "apply-function" 
      (is (= 0 (apply-function :cnt [])))
      (is (= 1 (apply-function :cnt [:x])))
      (is (= nil (apply-function :not-existing [])))
      (is (= nil (apply-function nil [])))
      ))

  (testing "generate-report"
    (let [
          w1 {:workflowitem-id 1}
          w2 {:workflowitem-id 2}

          r-basic-2 {:function :cnt :filter {:example {:workflowitem-id 2}}}
          r-nested-1 {:function :cnt :filter {:example {:workflowitem-id 1}} :children [{:function :cnt :filter {:example {:workflowitem-id 1}}}]}
          ]
      (is (= [1] (generate-report [r-basic-2] [w1 w2])))
      (is (= [1 [1]] (generate-report [r-nested-1] [w1 w2])))
      (is (= [2 [2] 1] (generate-report [r-nested-1 r-basic-2] [w1 w2 w1])))
      )
    )

  (testing "integrated-test"
    (let [
          id1t10 {:timestamp 10 :entityId 1 :workflowitem-id 2}
          id1t20 {:timestamp 20 :entityId 1 :workflowitem-id 2}
          id2t30 {:timestamp 30 :entityId 2 :workflowitem-id 2}
          r-basic-2 {:function :cnt :filter {:example {:workflowitem-id 2}}}
          full-descriptor {:reduce-function :last :result-descriptors [r-basic-2]}]
      (let [full-stream [id1t10 id1t20 id2t30]]
        (is (= [[1] [1] [2]] (run-analisis full-descriptor 10 full-stream)))
        (is (= [[2]] (run-analisis full-descriptor 100 full-stream)))
    )))

)
