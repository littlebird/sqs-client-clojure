(ns com.sprinklr.sqs-client-clojure
  (:import (com.amazonaws.services.sqs AmazonSQS
                                       AmazonSQSClientBuilder)
           (com.amazonaws.services.sqs.model AmazonSQSException
                                             CreateQueueRequest
                                             ListQueuesRequest
                                             Message
                                             SendMessageRequest
                                             SendMessageBatchRequest
                                             SendMessageBatchRequestEntry
                                             ReceiveMessageRequest)))

(def sqs (AmazonSQSClientBuilder/defaultClient))

(defn queue-url
  [queue-name]
  (some->> queue-name
           (.getQueueUrl sqs)
           (.getQueueUrl)))

(defmacro add-attribute
  [obj k v]
  `(.addAttributesEntry ~obj ~(name k) (str ~v)))

(defn create-queue
  ([queue-name] (create-queue queue-name {}))
  ([queue-name opts]
   (let [{:keys [delay-seconds retention-period fifo?]
          :or {delay-seconds 60
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
  [q-name]
  (some-> q-name
          (queue-url)
          (.deleteQueue sqs)))

(defn list-queues
  ([]
   (-> sqs
       (.listQueues)
       (.getQueueUrls)
       (->> (into []))))
  ([prefix]
   (-> sqs
       (.listQueues (ListQueuesRequest. prefix))
       (.getQueueUrls)
       (->> (into [])))))

(defn send-message
  ([queue-name msg] (send-message queue-name msg {}))
  ([queue-name msg opts]
   (let [{:keys [id wait-time]
          :or {id (hash msg)
               wait-time 0}} opts]
     (-> (SendMessageRequest.)
         (.withQueueUrl (queue-url queue-name))
         (.withMessageGroupId "sprinklrnewjobqueue")
         (.withMessageDeduplicationId (str id))
         (.withMessageBody msg)
         (.withDelaySeconds (int wait-time))
         (->> (.sendMessage sqs))))))

(defn receive-message
  [queue-name]
  (-> (ReceiveMessageRequest. (queue-url queue-name))
      (.withMaxNumberOfMessages (int 1))
      (.withWaitTimeSeconds (int 20))
      (->> (.receiveMessage sqs))
      (some-> (.getMessages)
              (first)
              (.getBody))))
