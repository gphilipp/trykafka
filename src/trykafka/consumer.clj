(ns trykafka.consumer
  (:require [clojure.data.json :as json])
  (:import (org.apache.kafka.common.serialization StringDeserializer ByteArrayDeserializer)
           (org.apache.kafka.clients.consumer KafkaConsumer)))


(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "avg-rate-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "true"
   "key.deserializer" StringDeserializer
   "value.deserializer" ByteArrayDeserializer})


(def consumer (doto
                (KafkaConsumer. c-cfg)
                (.subscribe ["deals"])))

(defn do-something [deal-name]
  (println (str "process " deal-name)))


(defn process-record [record]
  (do (println record)
      (let [m (-> record
                (.value)
                (String.)
                (json/read-str :key-fn keyword))]
        (do-something (:deal-name m)))))


(while true
  (let [records (.poll consumer 100)]
    (doseq [record records]
      (process-record record))))
