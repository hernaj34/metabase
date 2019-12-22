(ns metabase.driver.elastic
  (:require [clj-http.client :as http]
            [clojure
             [set :as set]
             [string :as str]]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
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
            [metabase.mbql
             [schema :as mbql.s]
             [util :as mbql.u]]
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
                           "X-Metabase-User"   user})}
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


(defmethod sql.qp/field->alias :elastic [_ field]
  nil)

;(defn parse-columns [columns] (vec (map (fn [col] (col :name)) columns)))
(defn parse-columns [columns] (vec (map (fn [col] {:name (u/qualified-name (col :name)) :type (col :type)}) columns)))

(defn- fetch-elastic-results!
  {:style/indent 1}
  [details query prev-cursor prev-cols prev-rows]
  {:pre [(map? details)]}
  (ssh/with-ssh-tunnel [details-with-tunnel details]
    (let [q1 (prn "Query:" query)
          {{:keys [columns rows cursor]} :body}
          (if prev-cursor
            (http/post (details->uri details-with-tunnel "/_sql?format=json&pretty")
                       (assoc (details->request details-with-tunnel)
                              :body (json/write-str {:cursor prev-cursor}), :as :json, :content-type :json, :redirect-strategy :lax))
            (http/post (details->uri details-with-tunnel "/_sql?format=json&pretty")
                       (assoc (details->request details-with-tunnel)
                              :body (json/write-str {:query query}), :as :json, :content-type :json, :redirect-strategy :lax)))
          q2 (prn "Cursor:" cursor)
          columns (if columns (parse-columns columns) prev-cols)]
      (if cursor (fetch-elastic-results! details query cursor columns (concat prev-rows rows))
          {:columns  columns
           :rows (concat prev-rows rows)}))))

(defn- execute-elastic-query!
  {:style/indent 1}
  [details query]
  {:pre [(map? details)]}
  (fetch-elastic-results! details query nil [] []))

(s/defmethod driver/can-connect? :elastic
  [driver {:keys [catalog] :as details} :- ElasticConnectionDetails]
  (let [{[[v]] :rows} (execute-elastic-query! details
                                              "select 1 + 1")]
    (= v 2)))

(defmethod driver/date-add :elastic
  [_ dt amount unit]
  (hsql/call :date_add (hx/literal unit) amount dt))

(s/defn database->get-tables
  "Return a set of all schema names in this `database`."
  [driver {{:keys [catalog schema] :as details} :details :as database}]
  (let [sql            "SHOW TABLES"
        {:keys [rows]} (execute-elastic-query! details sql)]
    (def tables {:tables (set (map (fn [row] {:name (get row 0) :schema nil}) rows))}))

  tables)
; (defn- describe-schema [driver {{:keys [catalog] :as details} :details} {:keys [schema]}]
;   (let [sql            (str "SHOW TABLES FROM " (sql.u/quote-name driver :schema catalog schema))
;         {:keys [rows]} (execute-elastic-query! details sql)
;         tables         (map first rows)]
;     (set (for [table-name tables]
;            {:name table-name, :schema schema}))))

(def ^:private excluded-schemas #{"information_schema"})

(defmethod driver/describe-database :elastic
  [driver database]
  (database->get-tables driver database))

(defn- elastic-type->base-type [field-type]
  (condp re-matches field-type
    #"text"        :type/Text
    #"date"        :type/Date
    #"boolean"     :type/Boolean
    #"long"        :type/BigInteger
    #"LONG"        :type/BigInteger
    #"float"       :type/Float
    #"BOOLEAN"     :type/Boolean
    #"TYNINT"      :type/Integer
    #"SMALLINT"    :type/Integer
    #"INTEGER"     :type/Integer
    #"BIGINT"      :type/BigInteger
    #"REAL"        :type/Float
    #"DUBLE"       :type/Float
    #"DECIMAL.*"   :type/Decimal
    #"VARCHAR.*"   :type/Text
    #"CHAR.*"      :type/Text
    #"VARBINARY.*" :type/*
    #"JSON"        :type/Text       ; TODO - this should probably be Dictionary or something
    #"DATE"        :type/Date
    #"TIME"        :type/Time
    #"TIME.+"      :type/DateTime
    #"ARRAY"       :type/Array
    #"MAP"         :type/Dictionary
    #"ROW.*"       :type/*          ; TODO - again, but this time we supposedly have a schema
    #".*"          :type/*))

(defmethod driver/describe-table :elastic
  [driver {{:keys [catalog] :as details} :details} {schema :schema, table-name :name}]
  (let [sql            (str "DESCRIBE \"" table-name "\"")
        {:keys [rows]} (execute-elastic-query! details sql)]
    (def descr {:schema nil
                :name   table-name
                :fields (set (for [[name type] rows]
                               {:name          name
                                :database-type type
                                :base-type     (elastic-type->base-type type)}))}))
  descr)

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
  (format "'%sT%s%s'" (t/local-date t) (t/local-time t) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:elastic ZonedDateTime]
  [_ t]
  (format "'%sT%s%s'" (t/local-date t) (t/local-time t) (t/zone-id t)))

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

(defn- like-clause
  "Generate a SQL `LIKE` clause. `value` is assumed to be a `Value` object (a record type with a key `:value` as well as
  some sort of type info) or similar as opposed to a raw value literal."
  [driver field value options]
  ;; TODO - don't we need to escape underscores and percent signs in the pattern, since they have special meanings in
  ;; LIKE clauses? That's what we're doing with Druid...
  ;;
  ;; TODO - Postgres supports `ILIKE`. Does that make a big enough difference performance-wise that we should do a
  ;; custom implementation?

  [:like field (sql.qp/->honeysql driver value)])

(s/defn ^:private update-string-value :- mbql.s/value
  [value :- (s/constrained mbql.s/value #(string? (second %)) "string value"), f]
  (update value 1 f))

(defmethod sql.qp/->honeysql [:elastic :starts-with] [driver [_ field value options]]
  (like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str % \%)) options))

(defmethod sql.qp/->honeysql [:elastic :contains] [driver [_ field value options]]
  (like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str \% % \%)) options))

(defmethod sql.qp/->honeysql [:elastic :ends-with] [driver [_ field value options]]
  (like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str \% %)) options))

(defmethod sql.qp/date [:elastic :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:elastic :minute]          [_ _ expr] (hsql/call :date_trunc (hx/literal :minute) expr))
(defmethod sql.qp/date [:elastic :minute-of-hour]  [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:elastic :hour]            [_ _ expr] (hsql/call :date_trunc (hx/literal :hour) expr))
(defmethod sql.qp/date [:elastic :hour-of-day]     [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:elastic :day]             [_ _ expr] (hsql/call :date_trunc (hx/literal :day) expr))
;; elastic is ISO compliant, so we need to offset Monday = 1 to Sunday = 1
(defmethod sql.qp/date [:elastic :day-of-week]     [_ _ expr] (hsql/call :day_of_week expr))
(defmethod sql.qp/date [:elastic :day-of-month]    [_ _ expr] (hsql/call :day expr))
(defmethod sql.qp/date [:elastic :day-of-year]     [_ _ expr] (hsql/call :day_of_year expr))

;; Similar to DoW, sicne elastic is ISO compliant the week starts on Monday, we need to shift that to Sunday
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
  "select current_timestamp")

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
