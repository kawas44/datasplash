(ns datasplash.avro
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [datasplash.core :as core]
            [datasplash.api :as ds])
  (:import org.apache.beam.sdk.io.AvroIO
           org.apache.beam.sdk.values.PCollection
           java.io.InputStream
           [clojure.lang Named]
           [java.util Collection Map UUID]
           [org.apache.avro
            Schema$Parser Schema$ArraySchema Schema Schema$Field]
           [org.apache.avro.generic
            GenericContainer GenericData$Array GenericData$EnumSymbol
            GenericData$Record GenericRecordBuilder]
           java.nio.ByteBuffer))

;; Private Helpers

(defn- ^String mangle [^String n]
  (str/replace n \- \_))

(defn- ^String unmangle [^String n]
  (str/replace n \_ \-))

(defn schema?
  "True iff `schema` is an Avro `Schema` instance."
  [schema] (instance? Schema schema))

(defn ^:private named?
  "True iff `x` is something which may be provided as an argument to `name`."
  [x] (or (string? x) (instance? Named x)))

(defn ^:private schema-mangle
  "Mangle `named` forms and existing schemas."
  [form]
  (cond (named? form) (-> form name mangle)
        (schema? form) (json/parse-string (str form))
        :else form))

(defn ^:private clj->json
  "Parse Clojure data into a JSON schema."
  [schema] (json/generate-string (postwalk schema-mangle schema)))

(defn ^:private raw-schema?
  "True if schema `source` should be parsed as-is."
  [source]
  (or (instance? InputStream source)
      (and (string? source)
           (.lookingAt (re-matcher #"[\[\{\"]" source)))))

(defn ^:private parse-schema-raw
  [^Schema$Parser parser source]
  (if (instance? String source)
    (.parse parser ^String source)
    (.parse parser ^InputStream source)))

(defn ^:private parse-schema*
  {:tag `Schema}
  [& sources]
  (let [parser (Schema$Parser.)]
    (reduce (fn [_ source]
              (->> (cond (schema? source) (str source)
                         (raw-schema? source) source
                         :else (clj->json source))
                   (parse-schema-raw parser)))
            nil
            sources)))

(defn parse-schema
  "Parse Avro schemas in `source` and `sources`.  Each schema source may be a
JSON string, an input stream containing a JSON schema, a Clojure data structure
which may be converted to a JSON schema, or an already-parsed Avro schema
object.  The schema for each subsequent source may refer to the types defined in
the previous schemas.  The parsed schema from the final source is returned."
  {:tag `Schema}
  ([source] (if (schema? source) source (parse-schema* source)))
  ([source & sources] (apply parse-schema* source sources)))


(defn- dispatch-on-type-fields
  [^Schema schema]
  (when schema
    (let [base-type (-> schema (.getType) (.getName))
          logical-type (-> schema (.getProps) (.get "logicalType"))]
      (if logical-type
        {:type base-type :logical-type logical-type}
        {:type base-type}))))

(defn make-coercion-stack
  "Given a registry mapping Avro type specs to 2-arity coercion
  constructors, recursively build up a coercion stack which will go
  clj <-> avro, returning the root coercion object.

  (satisfies `SchemaCoercion`)"
  [type-registry]
  (fn stack [^Schema schema]
    (let [dispatch (dispatch-on-type-fields schema)
          ctor (or (get type-registry dispatch)
                   (when (contains? dispatch :logical-type)
                     (get type-registry (dissoc dispatch :logical-type))))]
      (if-not ctor
        (throw (ex-info "Failed to dispatch coersion!"
                        {:schema schema, :dispatch dispatch}))
        (ctor stack schema)))))

;; Protocols and Multimethods

(defprotocol SchemaCoercion
  (match-clj? [schema-type clj-data])
  (match-avro? [schema-type avro-data])
  (avro->clj [schema-type avro-data])
  (clj->avro [schema-type clj-data path]))


;; Validation

(defn class-name
  "Returns a human readable description of x's type"
  [x]
  ;; nil does not have a class
  (if x
    (.getCanonicalName (class x))
    "nil"))

(defn serialization-error-msg [x expected-type]
  (format "%s is not a valid type for %s"
          (class-name x)
          expected-type))

(defn validate-clj! [this x path expected-type]
  (when-not (match-clj? this x)
    (throw (ex-info (serialization-error-msg x expected-type)
                    {:path path, :data x}))))

;;;; Primitive Types

;;; Boolean

(defrecord BooleanType []
  SchemaCoercion
  (match-clj? [_ x] (boolean? x))
  (match-avro? [_ x] (boolean? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bool")
    x))

;;; Bytes

(defn- byte-buffer?
  [x]
  (instance? ByteBuffer x))

(def avro-bytes?
  "Returns true if the object is compatible with Avro bytes, false otherwise

  * byte[] - Valid only as a top level schema type
  * java.nio.ByteBuffer - Valid only as a nested type"
  (some-fn byte-buffer? bytes?))

(defrecord BytesType []
  SchemaCoercion
  (match-clj? [_ x] (avro-bytes? x))
  (match-avro? [_ x] (avro-bytes? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "bytes")
    x))

;;; Double

;; Note that clojure.core/float? recognizes both single and double precision floating point values.

(defn num-coercable?
  "Checks whether `x` can be coerced to a number with `coercion-fn`
  (such as `long`)."
  [x coercion-fn]
  (try
    (coercion-fn (bigint x))
    true
    (catch RuntimeException e
      false)))

(defrecord DoubleType []
  SchemaCoercion
  (match-clj? [_ x] (float? x))
  (match-avro? [_ x] (float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "double")
    x))

(defn single-float? [x]
  (instance? Float x))

(defrecord FloatType []
  SchemaCoercion
  (match-clj? [_ x] (single-float? x))
  (match-avro? [_ x] (single-float? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "float")
    x))

(defrecord IntType []
  SchemaCoercion
  (match-clj? [_ x] (num-coercable? x int))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "int")
    (int x)))

(defrecord LongType []
  SchemaCoercion
  (match-clj? [_ x] (num-coercable? x long))
  (match-avro? [_ x] (int? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "long")
    (long x)))

(defrecord StringType []
  SchemaCoercion
  (match-clj? [_ x] (string? x))
  (match-avro? [_ x] (instance? CharSequence x))
  (avro->clj [_ x] (str x))
  (clj->avro [this x path]
    (validate-clj! this x path "string")
    x))

(defrecord NullType []
  SchemaCoercion
  (match-clj? [_ x] (nil? x))
  (match-avro? [_ x] (nil? x))
  (avro->clj [_ x] x)
  (clj->avro [this x path]
    (validate-clj! this x path "nil")
    x))

(defrecord SchemalessType []
  SchemaCoercion
  (match-clj? [_ x]
    true)
  (match-avro? [_ x]
    true)
  (avro->clj [_ x] x)
  (clj->avro [_ x path] x))

;;;; Complex Types

(defrecord ArrayType [^Schema schema element-coercion]
  SchemaCoercion
  (match-clj? [_ x]
    (sequential? x))

  (match-avro? [_ x]
    (instance? GenericData$Array x))

  (avro->clj [_ java-collection]
    (mapv #(avro->clj element-coercion %) java-collection))

  (clj->avro [this clj-seq path]
    (validate-clj! this clj-seq path "array")

    (GenericData$Array. ^Schema schema
                        ^Collection
                        (map-indexed (fn [i x]
                                       (clj->avro element-coercion x (conj path i)))
                                     clj-seq))))

(defn ->ArrayType
  "Wrapper by which to construct a `ArrayType` which handles the
  structural recursion of building the handler stack so that the
  `ArrayType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (ArrayType. schema
              (schema->coercion
               (.getElementType ^Schema$ArraySchema schema))))

(defrecord EnumType [^Schema schema]
  SchemaCoercion
  (match-clj? [_ x]
    (or
     (string? x)
     (keyword? x)))

  (match-avro? [_ x]
    (instance? GenericData$EnumSymbol x))

  (avro->clj [_ avro-enum]
    (-> (str avro-enum)
        (unmangle)
        (keyword)))

  (clj->avro [this clj-keyword path]
    (validate-clj! this clj-keyword path "enum")
    (->> (name clj-keyword)
         (mangle)
         (GenericData$EnumSymbol. schema))))

(defn ->EnumType [_schema->coercion ^Schema schema]
  (EnumType. schema))

(defrecord MapType [^Schema schema value-coercion]
  SchemaCoercion
  (match-clj? [_ x]
    (map? x))

  (match-avro? [_ x]
    (instance? Map x))

  (avro->clj [_ avro-map]
    (into {}
          (map (fn [[k v]] [(str k) (avro->clj value-coercion v)]))
          avro-map))

  (clj->avro [this clj-map path]
    (validate-clj! this clj-map path "map")

    (into {}
          (map (fn [[k v]]
                 (when-not (string? k)
                   (throw (ex-info (format "%s (%s) is not a valid map key type, only string keys are supported"
                                           (class-name k)
                                           k)
                                   {:path path, :clj-data clj-map})))
                 [k (clj->avro value-coercion v (conj path k))]))
          clj-map)))

(defn ->MapType
  "Wrapper by which to construct a `MapType` which handles the
  structural recursion of building the handler stack so that the
  `MapType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (MapType. schema (schema->coercion (.getValueType schema))))

(defrecord RecordType [^Schema schema field->schema+coercion]
  SchemaCoercion
  (match-clj? [_ clj-map]
    (let [fields (.getFields schema)]
      (every? (fn [[field-key [^Schema$Field field field-coercion]]]
                (let [field-value (get clj-map field-key ::missing)]
                  (if (= field-value ::missing)
                    (.defaultValue field)
                    (match-clj? field-coercion field-value))))
              field->schema+coercion)))

  (match-avro? [_ avro-record]
    (cond
      (nil? avro-record) true

      (instance? GenericData$Record avro-record)
      (let [^GenericData$Record generic-data-record avro-record]
        (= schema (.getSchema generic-data-record)))))

  (avro->clj [_ avro-record]
    (when avro-record
      (into {}
            (comp (map first)
                  (map (fn [^Schema$Field field]
                         (let [field-name (.name field)
                               field-key (keyword (unmangle field-name))
                               [_ field-coercion :as entry] (get field->schema+coercion field-key)
                               value (.get ^GenericData$Record avro-record field-name)]
                           (when-not field-coercion
                             (throw (ex-info "Unable to deserialize field"
                                             {:field field
                                              :field-name field-name
                                              :field-key field-key
                                              :entry entry})))
                           [field-key (avro->clj field-coercion value)]))))
            (vals field->schema+coercion))))

  (clj->avro [_ clj-map path]
    (when-not (map? clj-map)
      (throw (ex-info (serialization-error-msg clj-map "record")
                      {:path path, :clj-data clj-map})))

    (let [record-builder (GenericRecordBuilder. schema)]
      (try
        (doseq [[k v] clj-map]
          (let [new-k (mangle (name k))
                field (.getField schema new-k)]
            (when-not field
              (throw (ex-info (format "Field %s not known in %s"
                                      new-k
                                      (.getName schema))
                              {:path path, :clj-data clj-map})))
            (let [[_ field-coercion] (get field->schema+coercion k)
                  new-v (clj->avro field-coercion v (conj path k))]
              (.set record-builder new-k new-v))))

        (.build record-builder)

        (catch org.apache.avro.AvroRuntimeException e
          (throw (ex-info (str (.getMessage e))
                          {:path path, :clj-data clj-map} e)))))))

(defn ->RecordType
  "Wrapper by which to construct a `RecordType` which handles the
  structural recursion of building the handler stack so that the
  `RecordType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (let [fields (into {}
                     (map (fn [^Schema$Field field]
                            [(keyword (unmangle (.name field)))
                             [field (schema->coercion (.schema field))]]))
                     (.getFields schema))]
    (RecordType. schema fields)))

(defn- match-union-type [coercion-types pred]
  (some #(when (pred %) %) coercion-types))

(defrecord UnionType [coercion-types schemas]
  SchemaCoercion
  (match-clj? [_ clj-data]
    (boolean (match-union-type coercion-types #(match-clj? % clj-data))))

  (match-avro? [_ avro-data]
    (boolean (match-union-type coercion-types #(match-avro? % avro-data))))

  (avro->clj [_ avro-data]
    (let [schema-type (match-union-type coercion-types #(match-avro? % avro-data))]
      (avro->clj schema-type avro-data)))

  (clj->avro [_ clj-data path]
    (if-let [schema-type (match-union-type coercion-types  #(match-clj? % clj-data))]
      (clj->avro schema-type clj-data path)
      (throw (ex-info (serialization-error-msg clj-data
                                               (->> schemas
                                                    (map #(.getType ^Schema %))
                                                    (str/join ", ")
                                                    (format "union [%s]")))
                      {:path path, :clj-data clj-data})))))

(defn ->UnionType
  "Wrapper by which to construct a `UnionType` which handles the
  structural recursion of building the handler stack so that the
  `UnionType` type can be pretty simple."
  [schema->coercion ^Schema schema]
  (let [schemas (->> (.getTypes schema)
                     (into []))
        coercions (->> schemas
                       (map schema->coercion)
                       (into []))]
    (UnionType. coercions schemas)))




(def ^{:const true
       :doc   "Provides handlers for all of Avro's fundamental types besides `fixed`.

  Fixed is unsupported."}

  +base-schema-type-registry+

  {;; Primitives
   {:type "boolean"} (fn [_ _] (BooleanType.))
   {:type "bytes"}   (fn [_ _] (BytesType.))
   {:type "double"}  (fn [_ _] (DoubleType.))
   {:type "float"}   (fn [_ _] (FloatType.))
   {:type "int"}     (fn [_ _] (IntType.))
   {:type "long"}    (fn [_ _] (LongType.))
   {:type "string"}  (fn [_ _] (StringType.))
   {:type "null"}    (fn [_ _] (NullType.))
   nil               (fn [_ _] (SchemalessType.))

   ;; Compounds
   {:type "array"}  ->ArrayType
   {:type "enum"}   ->EnumType
   {:type "map"}    ->MapType
   {:type "record"} ->RecordType
   {:type "union"}  ->UnionType

   ;; Unsupported
   {:type "fixed"} (fn [_ _] (throw (ex-info "The fixed type is unsupported" {})))})


(def write-avro-file-schema
  (merge
    core/base-schema
    {:schema {:docstr "Specifies an Avro schema as EDN."}
     :shemas {:docstr "Specifies a list of dependent Avro schemas as EDN."}}))

(defn write-avro-file-raw
  [to options ^PCollection pcoll]
  (let [[base-path filename] (core/split-path to)
        ; schema (parse-schema (:schema options))
        opts (-> options
                 (dissoc [:schema])
                 (assoc :label (str "write-avro-file-to-"
                                    (core/clean-filename to)))
                 (cond-> filename (assoc :prefix filename)))

        ; schema->avro-type (make-coercion-stack +base-schema-type-registry+)
        ; avro-type (schema->avro-type schema)

        ]
    (core/apply-transform pcoll (-> (AvroIO/writeGenericRecords (parse-schema (:schema options)))
                                  ; (AvroIO/writeCustomTypeToGenericRecords)
                                  ; (.withSchema schema)
                                  ; (.withFormatFunction (core/sfn ))
                                  (.to (or base-path "./")))
                        write-avro-file-schema opts)))

(defn write-avro-file-clj-transform
  [to options]
  (let [safe-opts (dissoc options :name)
        schema (parse-schema (:schema options))

        schema->avro-type (make-coercion-stack +base-schema-type-registry+)
        avro-type (schema->avro-type schema)
        ]
    (core/ptransform
      :write-avro-file-from-clj
      [^PCollection pcoll]
      (->> pcoll
           ;todo convert to avro class here
           (ds/map (fn [v]
                     (clj->avro avro-type v [])))
           (write-avro-file-raw to safe-opts)))))

(defn write-avro-file
  ([to pcoll] (write-avro-file to {} pcoll))
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-avro-file)]
     (core/apply-transform pcoll (write-avro-file-clj-transform to opts) core/named-schema opts))))

(comment
  (def u-schema (parse-schema
                  {:name "User"
                   :type "record"
                   :fields [{:name "name"            :type "string"}
                            {:name "favorite_number" :type ["long" "null"]}
                            {:name "favorite_color"  :type ["string" "null"]}]}))

  (def u-schema (avro/parse-schema
                  {:name "example", :type "record",
                   :fields [{:name "left", :type "string"}
                            {:name "right", :type "long"}]}))
  (->> {:left "foo" :right 42} (avro/binary-encoded u-schema))

  (def input "{\"name\": \"Alyssa\", \"favorite-number\": 256, \"favorite-color\": null}")
  (def json-input (json/parse-string input true))
  (def json-input {:name "Alice" :favorite-number 21 :favorite-color nil})
  (def json-input {"name" "Alice" "favorite-number" 21 "favorite-color" nil})

  (def coerce-user (make-coercion-stack +base-schema-type-registry+))

  (def user-type (coerce-user u-schema))
  (clj->avro user-type json-input [])

)
