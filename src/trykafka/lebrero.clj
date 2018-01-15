(ns trykafka.lebrero)

(->
  (.table builder "share-holders" "share-holder-store")
  (.filter (k/pred [key position]
             (= "NASDAQ" (:exchange position))))
  (.groupBy (k/kv-mapper [key position]
              (KeyValue/pair (:client position)
                #{(:id position)})))
  (.reduce (k/reducer [value1 value2]
             (set/union value1 value2))
    (k/reducer [value1 value2]
      (let [result (set/difference value1 value2)]
        (when-not (empty? result)
          result)))
    "us-share-holders"))








