(ns com.sprinklr.sqs-client-clojure
  (:require [clojure.string :as string]
            [cognitect.transit :as transit])
  (:import (java.io ByteArrayOutputStream
                    ByteArrayInputStream)
           (com.amazonaws.regions Regions)
           (com.amazonaws.services.sqs AmazonSQS
                                       AmazonSQSClientBuilder)
           (com.amazonaws.services.sqs.model AmazonSQSException
                                             CreateQueueRequest
                                             DeleteMessageRequest
                                             ListQueuesRequest
                                             Message
                                             SendMessageRequest
                                             SendMessageBatchRequest
                                             SendMessageBatchRequestEntry
                                             ReceiveMessageRequest)))

(defn make-region-symbol
  [region]
  (-> region
      (name)
      (string/upper-case)
      (string/replace #"-" "_")
      (->> (str "com.amazonaws.regions.Regions/"))
      (symbol)))

(defmacro get-sqs
  [region]
    `(-> (AmazonSQSClientBuilder/standard)
         (.withRegion ~(make-region-symbol region))
         (.build)))

(defn queue-url
  [sqs queue-name]
  (some->> queue-name
           (.getQueueUrl sqs)
           (.getQueueUrl)))

(defmacro add-attribute
  [obj k v]
  `(.addAttributesEntry ~obj ~(name k) (str ~v)))

(defn create-queue
  ([sqs queue-name] (create-queue sqs queue-name {}))
  ([sqs queue-name opts]
   (let [{:keys [delay-seconds retention-period fifo?]
          :or {delay-seconds 0
               retention-period 86400
               fifo? true}} opts
         create-request (-> (CreateQueueRequest. queue-name)
                            (add-attribute FifoQueue fifo?)
                            (add-attribute DelaySeconds delay-seconds)
                            (add-attribute MessageRetentionPeriod
                                           retention-period))]
     (try (.createQueue sqs create-request)
          (catch AmazonSQSException e
            (when (not= (.getErrorCode e) "QueueAlreadyExists")
              (throw e)))))))

(defn delete-queue
  [sqs q-name]
  (some->> q-name
           (queue-url sqs)
           (.deleteQueue sqs)))

(defn list-queues
  ([sqs]
   (-> sqs
       (.listQueues)
       (.getQueueUrls)
       (->> (into []))))
  ([sqs prefix]
   (-> sqs
       (.listQueues (ListQueuesRequest. prefix))
       (.getQueueUrls)
       (->> (into [])))))

(defn get-group
  []
  "jobqueue")

(defn send-message
  ([sqs queue-name msg] (send-message sqs queue-name msg {}))
  ([sqs queue-name msg opts]
   (let [{:keys [id wait-time group]
          :or {id (hash msg)
               wait-time 0
               group (get-group)}} opts]
     (-> (SendMessageRequest.)
         (.withQueueUrl (queue-url sqs queue-name))
         (.withMessageGroupId group)
         (.withMessageDeduplicationId (str id))
         (.withMessageBody msg)
         (.withDelaySeconds (int wait-time))
         (->> (.sendMessage sqs))))))

(defn delete-message
  [sqs queue-name handle]
  (try
   (let [queue (queue-url sqs queue-name)
         delete-rq (-> (DeleteMessageRequest.)
                       (.withQueueUrl queue)
                       (.withReceiptHandle handle))]
     (.deleteMessage sqs delete-rq))
   (catch Exception e
     (pr-str e))))

(defn receive-message
  [sqs queue-name]
  (let [rq-req (-> (ReceiveMessageRequest. (queue-url sqs queue-name))
                   (.withMaxNumberOfMessages (int 1))
                   (.withWaitTimeSeconds (int 20)))
        msg (some-> (.receiveMessage sqs rq-req)
                    (.getMessages)
                    (first))]
    (when msg
      (delete-message sqs queue-name (.getReceiptHandle msg)))
    (some-> msg
            (.getBody))))

(defn transit-string
  [data encoders]
  (let [baos (ByteArrayOutputStream. 512)
        writer (transit/writer baos :json encoders)
        _ (transit/write writer data)
        packed (.toByteArray baos)]
    (String. packed)))

(defn string-transit
  [string decoders]
  (let [bytes-in (ByteArrayInputStream. (.getBytes string))
        reader (transit/reader bytes-in :json decoders)]
    (transit/read reader)))
