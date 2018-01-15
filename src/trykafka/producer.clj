(ns trykafka.producer
  (:require [taoensso.nippy :as nippy]
            [clojure.data.json :as json])
  (:import (org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer StringSerializer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (java.io StringWriter)))

(def p-cfg {"value.serializer" ByteArraySerializer
            "key.serializer" StringSerializer
            "schema.enable" "false"
            "bootstrap.servers" "localhost:9092"})

(def deals [{:id "O12334" :deal-name "Great Northern Hotel" :deal-amount 3000}
            {:id "O12334" :deal-name "Double R" :deal-amount 1250}
            {:id "O12334" :deal-name "Palmers House" :deal-amount 2430}])

(def producer (KafkaProducer. p-cfg))

(defn send-deals [deals]
  (doseq [d deals]
    (let [sw   (StringWriter.)
          _    (json/write d sw)
          data (.toString sw)]
      (do (println data)
          (.send producer (ProducerRecord. "deals-v6" (:id d) (.getBytes data)))
          ))))

(send-deals [{:id "O12334" :deal-name "Paris" :deal-amount 6000}
             ])
;(.send producer (ProducerRecord. "connect-test" (nippy/freeze {:id "O1234" :deal-name "Accord Hotel"})))
