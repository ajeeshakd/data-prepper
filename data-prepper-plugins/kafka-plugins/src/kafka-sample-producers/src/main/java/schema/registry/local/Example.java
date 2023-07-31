package schema.registry.local;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Example extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 963322053341084311L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"org.opensearch.dataprepper.plugins.kafka.avro\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}");
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    private static SpecificData MODEL$ = new SpecificData();

    private static final BinaryMessageEncoder<Example> ENCODER =
            new BinaryMessageEncoder<Example>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<Example> DECODER =
            new BinaryMessageDecoder<Example>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     */
    public static BinaryMessageDecoder<Example> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     */
    public static BinaryMessageDecoder<Example> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<Example>(MODEL$, SCHEMA$, resolver);
    }

    /** Serializes this Example to a ByteBuffer. */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /** Deserializes a Example from a ByteBuffer. */
    public static Example fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    @Deprecated public CharSequence field1;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public Example() {}

    /**
     * All-args constructor.
     * @param field1 The new value for field1
     */
    public Example(CharSequence field1) {
        this.field1 = field1;
    }

    public org.apache.avro.Schema getSchema() { return SCHEMA$; }
    // Used by DatumWriter.  Applications should not call.
    public Object get(int field$) {
        switch (field$) {
            case 0: return field1;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: field1 = (CharSequence)value$;
            break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'field1' field.
     * @return The value of the 'field1' field.
     */
    public CharSequence getField1() {
        return field1;
    }

    /**
     * Sets the value of the 'field1' field.
     * @param value the value to set.
     */
    public void setField1(CharSequence value) {
        this.field1 = value;
    }

    /**
     * Creates a new Example RecordBuilder.
     * @return A new Example RecordBuilder
     */
    public static schema.registry.local.Example.Builder newBuilder() {
        return new schema.registry.local.Example.Builder();
    }

    /**
     * Creates a new Example RecordBuilder by copying an existing Builder.
     * @param other The existing builder to copy.
     * @return A new Example RecordBuilder
     */
    public static schema.registry.local.Example.Builder newBuilder(schema.registry.local.Example.Builder other) {
        return new schema.registry.local.Example.Builder(other);
    }

    /**
     * Creates a new Example RecordBuilder by copying an existing Example instance.
     * @param other The existing instance to copy.
     * @return A new Example RecordBuilder
     */
    public static schema.registry.local.Example.Builder newBuilder(schema.registry.local.Example other) {
        return new schema.registry.local.Example.Builder(other);
    }

    /**
     * RecordBuilder for Example instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Example>
            implements org.apache.avro.data.RecordBuilder<Example> {

        private CharSequence field1;

        /** Creates a new Builder */
        private Builder() {
            super(SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * @param other The existing Builder to copy.
         */
        private Builder(schema.registry.local.Example.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.field1)) {
                this.field1 = data().deepCopy(fields()[0].schema(), other.field1);
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing Example instance
         * @param other The existing instance to copy.
         */
        private Builder(schema.registry.local.Example other) {
            super(SCHEMA$);
            if (isValidValue(fields()[0], other.field1)) {
                this.field1 = data().deepCopy(fields()[0].schema(), other.field1);
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Gets the value of the 'field1' field.
         * @return The value.
         */
        public CharSequence getField1() {
            return field1;
        }

        /**
         * Sets the value of the 'field1' field.
         * @param value The value of 'field1'.
         * @return This builder.
         */
        public schema.registry.local.Example.Builder setField1(CharSequence value) {
            validate(fields()[0], value);
            this.field1 = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'field1' field has been set.
         * @return True if the 'field1' field has been set, false otherwise.
         */
        public boolean hasField1() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'field1' field.
         * @return This builder.
         */
        public schema.registry.local.Example.Builder clearField1() {
            field1 = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Example build() {
            try {
                Example record = new Example();
                record.field1 = fieldSetFlags()[0] ? this.field1 : (CharSequence) defaultValue(fields()[0]);
                return record;
            } catch (Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<Example>
            WRITER$ = (org.apache.avro.io.DatumWriter<Example>)MODEL$.createDatumWriter(SCHEMA$);

    @Override public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<Example>
            READER$ = (org.apache.avro.io.DatumReader<Example>)MODEL$.createDatumReader(SCHEMA$);

    @Override public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

}
