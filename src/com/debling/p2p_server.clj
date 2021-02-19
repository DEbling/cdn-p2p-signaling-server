(ns com.debling.p2p-server
  (:gen-class)
  (:require
   [org.httpkit.server           :as s]
   [reitit.ring                  :as ring]
   [reitit.spec                  :as rs]
   [reitit.dev.pretty            :as pretty]
   [clojure.data.json            :as json]
   [clojure.tools.logging        :as log]
   [clojure.java.io              :as io]
   [clojure.string               :as string]
   [ring.middleware.cors         :refer [wrap-cors]]
   [ring.middleware.resource     :as ring-res]
   [ring.middleware.content-type :as ring-content]
   [iapetos.core                 :as prometheus]
   [iapetos.collector.ring       :as prom-ring]
   [iapetos.collector.jvm        :as prom-jvm]
   [com.debling.db               :as db]))

(set! *warn-on-reflection* true)

(defonce registry
  (-> (prometheus/collector-registry)
      prom-jvm/initialize
      prom-ring/initialize
      (prometheus/register
       (prometheus/gauge :p2p-server/p2p-clients))))

(defonce server (atom nil))

;; Referencia atomica para um conjunto que guarda todos os peers na rede
(defonce peers (atom {}))


(defn broadcast
  [msg ch-to-exclude]
  (let [other-peers (->> @peers keys (remove #(= ch-to-exclude %1)))]
  (log/debug "Broadcasting msg " msg)
    (run! #(s/send! %1 (json/write-str msg)) other-peers)))


(defn on-connect!
  "Handler para o evento de conecção, adicionar peer ao conjunto
  global de peers"
  [ch]
  (let [client-id (java.util.UUID/randomUUID)]
    (log/info "New peer connected, assigned id " client-id)
    (swap! peers assoc ch client-id)
    (prometheus/inc registry :p2p-server/p2p-clients)

    ;; Envia o id do cliente para o Peer que se conectou
    (s/send! ch (json/write-str {:kind :connect
                                 :clientId (str client-id)}))
    ;; Faz broadcast para todos os outros cliente, notificando-os
    ;; sobre o surgimento do novo cliente
    (broadcast {:kind :user-connected
                :clientId (str client-id)}
               ch)))

(defn disconnect!
  "Remover ao disconnectar"
  [ch status]
  (log/info "channel closed: " status)
  (swap! peers dissoc ch)
  (prometheus/dec registry :p2p-server/p2p-clients))

(defmulti wsmsg-handler! #(:kind %2))

(defmethod wsmsg-handler! "offer" [ch msg]
  (let [other-peers (->> @peers keys (remove #(= ch %1)))]
    (run! #(s/send! %1 (json/write-str msg)) other-peers)))

(defmethod wsmsg-handler! "answer" [ch msg]
  (let [other-peers (->> @peers keys (remove #(= ch %1)))]
    (run! #(s/send! %1 (json/write-str msg)) other-peers)))

(defmethod wsmsg-handler! "new-ice-candidate" [ch msg]
  (log/info "new-ice-candidate")
  (let [other-peers (->> @peers keys (remove #(= ch %1)))]
    (run! #(s/send! %1 (json/write-str msg)) other-peers)))

(defmethod wsmsg-handler! :default [ch msg]
  (prn msg)
  (log/error "no handler for msg " msg))

(defn ws-handler
  "Handler que trata de coneções WebSocket, que são usadas para coordenação de
  clientes"
  [req]
  (assert (:websocket? req))
  (s/as-channel req {:on-open    on-connect!
                     :on-receive #(wsmsg-handler! %1 (json/read-str %2 :key-fn keyword))
                     :on-close   disconnect!}))

(defn stop-server! []
  (when-not (nil? @server)
    (log/info "Shutdown started waiting for requests to finish")
    (@server :timeout 100)
    (log/info "Server shutdown completed!")
    (reset! server nil)))


(defn- wrap-request-logging [handler]
  (fn [{:keys [request-method uri] :as req}]
    (let [resp (handler req)]
      (log/info (name request-method) (:status resp)
            (if-let [qs (:query-string req)]
              (str uri "?" qs) uri))
      resp)))

(defn metrics-handler [req]
  (let [metrics (-> req :body io/reader (json/read :key-fn keyword))]
    (db/insert-metric! metrics)
    {:status  200
     :headers {"Content-Type" "application/json"}}))

(defn cors [app]
  (wrap-cors app
             :access-control-allow-origin [#".*"]
             :access-control-allow-methods [:get :put :post :delete]))


(def app (ring/ring-handler
             (ring/router
              [["/ws" ws-handler]
               ["/metrics" {:post metrics-handler}]]
              {:validate  rs/validate
               :exception pretty/exception})))


(def app-handler (-> app
                     wrap-request-logging
                     (ring-res/wrap-resource "/public")
                     ring-content/wrap-content-type
                     cors))

(defn -main [& args]
  (let [port (Integer/valueOf (or (System/getenv "port") "8080"))]
    (reset! server
            (s/run-server #'app-handler
                          {:port port}))
    (log/info "Server listening on port" port)))
