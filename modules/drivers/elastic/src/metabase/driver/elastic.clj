(ns metabase.driver.elastic
  (:require [clj-http.client :as http]
            [clojure
             [set :as set]
             [string :as str]]
            [clojure.tools.logging :as log]
            [honeysql
             [core :as hsql]
             [helpers :as h]]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor
             [store :as qp.store]
             [timezone :as qp.timezone]
             [util :as qputil]]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]
             [i18n :refer [trs tru]]
             [schema :as su]
             [ssh :as ssh]]
            [schema.core :as s])
  (:import java.sql.Time
           [java.time OffsetDateTime ZonedDateTime]))

(driver/register! :elastic, :parent :sql)

;;; Elastic API helpers

(def ^:private ElasticConnectionDetails
  {:host    su/NonBlankString
   :port    (s/cond-pre su/NonBlankString su/IntGreaterThanZero)
   :catalog su/NonBlankString
   s/Any    s/Any})

(s/defn ^:private details->uri
  [{:keys [ssl host port]} :- ElasticConnectionDetails, path]
  (str (if ssl "https" "http") "://" host ":" port
       path))

(defn- details->request [{:keys [user password catalog]}]
  (merge {:headers (merge {"content-type" "application/json"
                           "X-Metabase-User"   user}
                    )}
         (when password
           {:basic-auth [user password]})))

(defn ^:private create-cancel-url [cancel-uri host port info-uri]
  ;; Replace the host in the cancel-uri with the host from the info-uri provided from the presto response- this doesn't
  ;; break SSH tunneling as the host in the cancel-uri is different if it's enabled
  (str/replace cancel-uri (str host ":" port) (get (str/split info-uri #"/") 2)))

(defn- field-type->parser [field-type]
  (condp re-matches field-type
    #"decimal.*"                bigdec
    #"time"                     #(u.date/parse % (qp.timezone/results-timezone-id))
    #"time with time zone"      #(u.date/parse % (qp.timezone/results-timezone-id))
    #"timestamp"                #(u.date/parse % (qp.timezone/results-timezone-id))
    #"timestamp with time zone" #(u.date/parse % (qp.timezone/results-timezone-id))
    #".*"                       identity))

(defn- parse-elastic-results [columns data]
  (let [parsers (map (comp field-type->parser :type) columns)]
    (for [row data]
      (vec
       (for [[value parser] (partition 2 (interleave row parsers))]
         (u/prog1 (when (some? value)
                    (parser value))
           (log/tracef "Parse %s -> %s" (pr-str value) (pr-str <>))))))))

(defn- fetch-elastic-results! [details {prev-columns :columns, prev-rows :rows} uri]
  (let [{{:keys [columns data nextUri error]} :body} (http/get uri (assoc (details->request details) :as :json))]
    (when error
      (throw (ex-info (or (:message error) (tru "Error running query.")) error)))
    (let [rows    (parse-elastic-results columns data)
          results {:columns (or columns prev-columns)
                   :rows    (vec (concat prev-rows rows))}]
      (if (nil? nextUri)
        results
        (do (Thread/sleep 100)        ; Might not be the best way, but the pattern is that we poll Presto at intervals
            (fetch-elastic-results! details results nextUri))))))

(defn- execute-elastic-query!
  {:style/indent 1}
  [details query]
  {:pre [(map? details)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (let [{{:keys [columns data nextUri error id infoUri]} :body}
          (http/post (details->uri details-with-tunnel "/_xpack/sql?format=json&pretty")
                     (assoc (details->request details-with-tunnel)
                             :body query, :as :json, :redirect-strategy :lax))]
      (when error
        (throw (ex-info (or (:message error) "Error preparing query.") error)))
      (let [rows    (parse-elastic-results (or columns []) (or data []))
            results {:columns (or columns [])
                     :rows    rows}]
        (if (nil? nextUri)
          results
          ;; When executing the query, it doesn't return the results, but is geared toward async queries. After
          ;; issuing the query, the below will ask for the results. Asking in a future so that this thread can be
          ;; interrupted if the client disconnects
          (let [results-future (future (fetch-elastic-results! details-with-tunnel results nextUri))]
            (try
              @results-future
              (catch InterruptedException e
                (try
                  (if id
                    ;; If we have a query id, we can cancel the query
                    (try
                      (let [tunneledUri (details->uri details-with-tunnel (str "/_xpack/sql?format=json&pretty" id))
                            adjustedUri (create-cancel-url tunneledUri (get details :host) (get details :port) infoUri)]
                        (http/delete adjustedUri (details->request details-with-tunnel)))
                      ;; If we fail to cancel the query, log it but propogate the interrupted exception, instead of
                      ;; covering it up with a failed cancel
                      (catch Exception e
                        (log/error e (trs "Error canceling query with ID {0}" id))))
                    (log/warn (trs "Client connection closed, no query-id found, can't cancel query")))
                  (finally
                    ;; Propagate the error so that any finalizers can still run
                    (throw e)))))))))))


;;; `:sql` driver implementation

(s/defmethod driver/can-connect? :elastic
  [driver {:keys [catalog] :as details} :- ElasticConnectionDetails]
  (let [{[[v]] :rows} (execute-elastic-query! details
                        (format "SHOW SCHEMAS FROM %s LIKE 'information_schema'" (sql.u/quote-name driver :database catalog)))]
    (= v "information_schema")))

(defmethod driver/date-add :elastic
  [_ dt amount unit]
  (hsql/call :date_add (hx/literal unit) amount dt))

(s/defn ^:private database->all-schemas :- #{su/NonBlankString}
  "Return a set of all schema names in this `database`."
  [driver {{:keys [catalog schema] :as details} :details :as database}]
  (let [sql            (str "SHOW SCHEMAS FROM " (sql.u/quote-name driver :database catalog))
        {:keys [rows]} (execute-elastic-query! details sql)]
    (set (map first rows))))

(defn- describe-schema [driver {{:keys [catalog] :as details} :details} {:keys [schema]}]
  (let [sql            (str "SHOW TABLES FROM " (sql.u/quote-name driver :schema catalog schema))
        {:keys [rows]} (execute-elastic-query! details sql)
        tables         (map first rows)]
    (set (for [table-name tables]
           {:name table-name, :schema schema}))))

(def ^:private excluded-schemas #{"information_schema"})

(defmethod driver/describe-database :elastic
  [driver database]
  (let [schemas (remove excluded-schemas (database->all-schemas driver database))]
    {:tables (reduce set/union (for [schema schemas]
                                 (describe-schema driver database {:schema schema})))}))

(defn- elastic-type->base-type [field-type]
  (condp re-matches field-type
    #"boolean"     :type/Boolean
    #"tinyint"     :type/Integer
    #"smallint"    :type/Integer
    #"integer"     :type/Integer
    #"bigint"      :type/BigInteger
    #"real"        :type/Float
    #"double"      :type/Float
    #"decimal.*"   :type/Decimal
    #"varchar.*"   :type/Text
    #"char.*"      :type/Text
    #"varbinary.*" :type/*
    #"json"        :type/Text       ; TODO - this should probably be Dictionary or something
    #"date"        :type/Date
    #"time"        :type/Time
    #"time.+"      :type/DateTime
    #"array"       :type/Array
    #"map"         :type/Dictionary
    #"row.*"       :type/*          ; TODO - again, but this time we supposedly have a schema
    #".*"          :type/*))

(defmethod driver/describe-table :elastic
  [driver {{:keys [catalog] :as details} :details} {schema :schema, table-name :name}]
  (let [sql            (str "DESCRIBE " (sql.u/quote-name driver :table catalog schema table-name))
        {:keys [rows]} (execute-elastic-query! details sql)]
    {:schema schema
     :name   table-name
     :fields (set (for [[name type] rows]
                    {:name          name
                     :database-type type
                     :base-type     (elastic-type->base-type type)}))}))

(defmethod sql.qp/->honeysql [:elastic String]
  [_ s]
  (hx/literal (str/replace s "'" "''")))

(defmethod sql.qp/->honeysql [:elastic Boolean]
  [_ bool]
  (hsql/raw (if bool "TRUE" "FALSE")))

(defmethod sql.qp/->honeysql [:elastic :stddev]
  [driver [_ field]]
  (hsql/call :stddev_samp (sql.qp/->honeysql driver field)))

(defmethod sql.qp/->honeysql [:elastic :time]
  [_ [_ t]]
  (hx/cast :time (u.date/format-sql (t/local-time t))))

;; See https://elasticdb.io/docs/current/functions/datetime.html

;; This is only needed for test purposes, because some of the sample data still uses legacy types
(defmethod unprepare/unprepare-value [:elastic Time]
  [driver t]
  (unprepare/unprepare-value driver (t/local-time t)))

(defmethod unprepare/unprepare-value [:elastic OffsetDateTime]
  [_ t]
  (format "timestamp '%s %s %s'" (t/local-date t) (t/local-time t) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:elastic ZonedDateTime]
  [_ t]
  (format "timestamp '%s %s %s'" (t/local-date t) (t/local-time t) (t/zone-id t)))

(defmethod driver/execute-query :elastic
  [driver {database-id                  :database
           :keys                        [settings]
           {sql :query, params :params} :native
           query-type                   :type
           :as                          outer-query}]
  (let [sql                    (str "-- "
                                    (qputil/query->remark outer-query) "\n"
                                    (unprepare/unprepare driver (cons sql params)))
        details                (merge (:details (qp.store/database))
                                      settings)
        {:keys [columns rows]} (execute-elastic-query! details sql)
        columns                (for [[col name] (map vector columns (map :name columns))]
                                 {:name name, :base_type (elastic-type->base-type (:type col))})]
    (merge
     {:columns (map (comp u/qualified-name :name) columns)
      :rows    rows}
     ;; only include `:cols` info for native queries for the time being, since it changes all the types up for MBQL
     ;; queries (e.g. `:count` aggregations come back as `:type/BigInteger` instead of `:type/Integer`.) I don't want
     ;; to deal with fixing a million tests to make it work at this second since it doesn't make a difference from an
     ;; FE perspective. Perhaps when we get our test story sorted out a bit better we can fix this
     (when (= query-type :native)
       {:cols columns}))))

(defmethod driver/humanize-connection-error-message :elastic
  [_ message]
  (condp re-matches message
    #"^java.net.ConnectException: Connection refused.*$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^clojure.lang.ExceptionInfo: Catalog .* does not exist.*$"
    (driver.common/connection-error-messages :database-name-incorrect)

    #"^java.net.UnknownHostException.*$"
    (driver.common/connection-error-messages :invalid-hostname)

    #".*" ; default
    message))

;;; `:sql-driver` methods

(defmethod sql.qp/apply-top-level-clause [:elastic :page]
  [_ _ honeysql-query {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (h/limit honeysql-query items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause (format "row_number() OVER (%s)"
                                (first (hsql/format (select-keys honeysql-query [:order-by])
                                                    :allow-dashed-names? true
                                                    :quoting :ansi)))]
        (-> (apply h/select (map last (:select honeysql-query)))
            (h/from (h/merge-select honeysql-query [(hsql/raw over-clause) :__rownum__]))
            (h/where [:> :__rownum__ offset])
            (h/limit items))))))

(defmethod sql.qp/date [:elastic :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:elastic :minute]          [_ _ expr] (hsql/call :date_trunc (hx/literal :minute) expr))
(defmethod sql.qp/date [:elastic :minute-of-hour]  [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:elastic :hour]            [_ _ expr] (hsql/call :date_trunc (hx/literal :hour) expr))
(defmethod sql.qp/date [:elastic :hour-of-day]     [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:elastic :day]             [_ _ expr] (hsql/call :date_trunc (hx/literal :day) expr))
;; Presto is ISO compliant, so we need to offset Monday = 1 to Sunday = 1
(defmethod sql.qp/date [:elastic :day-of-week]     [_ _ expr] (hx/+ (hx/mod (hsql/call :day_of_week expr) 7) 1))
(defmethod sql.qp/date [:elastic :day-of-month]    [_ _ expr] (hsql/call :day expr))
(defmethod sql.qp/date [:elastic :day-of-year]     [_ _ expr] (hsql/call :day_of_year expr))

;; Similar to DoW, sicne Presto is ISO compliant the week starts on Monday, we need to shift that to Sunday
(defmethod sql.qp/date [:elastic :week]
  [_ _ expr]
  (hsql/call :date_add
    (hx/literal :day) -1 (hsql/call :date_trunc
                           (hx/literal :week) (hsql/call :date_add
                                                (hx/literal :day) 1 expr))))

;; Offset by one day forward to "fake" a Sunday starting week
(defmethod sql.qp/date [:elastic :week-of-year]
  [_ _ expr]
  (hsql/call :week (hsql/call :date_add (hx/literal :day) 1 expr)))

(defmethod sql.qp/date [:elastic :month]           [_ _ expr] (hsql/call :date_trunc (hx/literal :month) expr))
(defmethod sql.qp/date [:elastic :month-of-year]   [_ _ expr] (hsql/call :month expr))
(defmethod sql.qp/date [:elastic :quarter]         [_ _ expr] (hsql/call :date_trunc (hx/literal :quarter) expr))
(defmethod sql.qp/date [:elastic :quarter-of-year] [_ _ expr] (hsql/call :quarter expr))
(defmethod sql.qp/date [:elastic :year]            [_ _ expr] (hsql/call :date_trunc (hx/literal :year) expr))

(defmethod sql.qp/unix-timestamp->timestamp [:elastic :seconds] [_ _ expr]
  (hsql/call :from_unixtime expr))

(defmethod driver.common/current-db-time-date-formatters :elastic [_]
  (driver.common/create-db-time-formatters "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

(defmethod driver.common/current-db-time-native-query :elastic [_]
  "select to_iso8601(current_timestamp)")

(defmethod driver/current-db-time :elastic [& args]
  (apply driver.common/current-db-time args))

(defmethod driver/supports? [:elastic :set-timezone]                    [_ _] true)
(defmethod driver/supports? [:elastic :basic-aggregations]              [_ _] true)
(defmethod driver/supports? [:elastic :standard-deviation-aggregations] [_ _] true)
(defmethod driver/supports? [:elastic :expressions]                     [_ _] true)
(defmethod driver/supports? [:elastic :native-parameters]               [_ _] true)
(defmethod driver/supports? [:elastic :expression-aggregations]         [_ _] true)
(defmethod driver/supports? [:elastic :binning]                         [_ _] true)

(defmethod driver/supports? [:elastic :foreign-keys] [_ _] true)
