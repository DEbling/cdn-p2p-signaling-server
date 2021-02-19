(ns com.debling.db
  (:require
   [next.jdbc :as jdbc])
  (:import
   [java.time Instant]
   [java.sql Timestamp]))

(def db-spec {:dbtype   "postgresql"
              :dbname   "cdn_p2p"
              :host     "localhost"
              :user     "postgres"
              :password ""})

(def ds (jdbc/get-datasource db-spec))


(def schema "
CREATE TABLE IF NOT EXISTS metrics
( id         SERIAL PRIMARY KEY
, peer_id    TEXT NOT NULL
, method     TEXT NOT NULL
, segment    TEXT NOT NULL
, start_time TIMESTAMP WITH TIME ZONE NOT NULL
, end_time   TIMESTAMP WITH TIME ZONE NOT NULL
)")

(jdbc/execute! ds [schema])

(defn str->sql-timestamp [s]
  (-> s Instant/parse Timestamp/from))

(defn insert-metric! [metrics-map]
  (let [{:keys [peerId method segment startTime endTime]} metrics-map]
    (jdbc/execute! ds ["
INSERT INTO METRICS(peer_id, method, segment, start_time, end_time)
VALUES (?, ?, ?, ?, ?)"
                       peerId method segment
                       (str->sql-timestamp startTime)
                       (str->sql-timestamp endTime)])))
