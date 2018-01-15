(ns trykafka.streams
  (:require [clojure.data.json :as json])
  (:import (org.apache.kafka.streams.kstream KStreamBuilder Reducer KStream)
           (org.apache.kafka.streams KafkaStreams)
           (org.apache.kafka.common.serialization ByteArrayDeserializer StringDeserializer LongDeserializer)
           (java.util Properties)))


(defn deals-topology []
  (let [builder (KStreamBuilder.)
        deals   (.stream builder (into-array ["deals-v6"]))]
    (.. deals
      (groupByKey)
      (reduce (reify Reducer
                (apply [_ v1 v2]
                  (let [current-amount (Long/parseLong (String. v1))
                        deal           (json/read-str (String. v2) :key-fn keyword)
                        _              (println deal)
                        deal-amount    (:deal-amount deal)
                        _              (println (type deal-amount))]
                    (do (println "adding amount of " deal " to " current-amount)
                        (.getBytes (str (+ current-amount deal-amount)))))))
        "deal-id")
      (to "total-amounts-v2"))
    builder))

(def config
  {"application.id" "hicks"
   "bootstrap.servers" "localhost:9092"
   "auto.offset.reset" "earliest"
   "key.deserializer" StringDeserializer
   "value.deserializer" LongDeserializer
   })

(let [props (doto (Properties.) (.putAll config))]
  (.start (KafkaStreams. (deals-topology) props)))