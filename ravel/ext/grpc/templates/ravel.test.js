/* ravel.test.js
 *
 * Unit tests for Schema, Fields, and GrpcService.
 * Runs under Jest, the testing framework.
 */

const {
    Schema,
    Field,
    DateTimeField,
    SchemaField,
    JsonObjectField,
    ListField,
} = require('./ravel.js');


class DummyMessage {
    constructor(a = null, b = null) {
        this.a = a;
        this.b = b;
    }
    getA() {
        return this.a;
    }
    getB() {
        return this.b;
    }
    setA(value) {
        this.a = value;
    }
    setB(value) {
        this.b = value;
    }
}


const dummySchema = new Schema('Dummy', {
    a: new Field('a'),
    b: new Field('b')
});

const context = {
    'schemas': {
        'Dummy': dummySchema,
    },
    'messageTypes': {
        'Dummy': DummyMessage,
    }
}


test('schema calls dump on all fields', () => {
    const message = dummySchema.dump({'a': 1, 'b': 'foo'}, context)
    expect(message.a).toBe(1);
    expect(message.b).toBe('foo');
});

test('schema calls load on all fields', () => {
    const message = new DummyMessage(1, 'foo')
    const obj = dummySchema.load(message, context);
    expect(obj.a).toBe(1);
    expect(obj.b).toBe('foo');
});

test('DateTime field loads from UNIX timestamp', () => {
    const date = new Date(1995, 11, 17, 3, 24, 0);
    const timeStamp = date.getTime() / 1000;
    const field = new DateTimeField('created_at');
    const result = field.load(timeStamp, context);
    expect(result).toBeInstanceOf(Date);
    expect(result.getTime()).toBe(timeStamp * 1000);
});

test('DateTime field loads from string', () => {
    const date = new Date(1995, 11, 17, 3, 24, 0);
    const dateTimeString = date.toString();
    const field = new DateTimeField('created_at');
    const result = field.load(dateTimeString, context);
    expect(result).toBeInstanceOf(Date);
    expect(result.getTime()).toBe(date.getTime());
});

test('DateTime field dumps from string', () => {
    const field = new DateTimeField('created_at');
    const now = new Date();
    const value = field.dump(now.toString(), context);
    expect(value).toBe(Math.floor(now.getTime() / 1000));
});

test('DateTime field dumps from Date', () => {
    const field = new DateTimeField('created_at');
    const now = new Date();
    const value = field.dump(now, context);
    expect(value).toBe(now.getTime() / 1000);
});

test('DateTime field dumps from number', () => {
    const field = new DateTimeField('created_at');
    const now = new Date();
    const value = field.dump(now.getTime(), context);
    expect(value).toBe(now.getTime() / 1000);
});

test('List field loads values', () => {
    const field = new ListField('names', new DateTimeField());
    const now = new Date();
    const unixTimestamp = now.getTime() / 1000;
    const values = [unixTimestamp, unixTimestamp];
    const loadedValues = field.load(values, context);
    expect(loadedValues).toStrictEqual([now, now]);
});

test('List field dumps values', () => {
    const field = new ListField('names', new DateTimeField());
    const now = new Date();
    const unixTimestamp = now.getTime() / 1000;
    const values = [now, now]
    const dumpedValues = field.dump(values, context);
    expect(dumpedValues).toStrictEqual([unixTimestamp, unixTimestamp]);
});

test('JsonObjectField loads from string', () => {
    const field = new JsonObjectField('data');
    const preloadedValue = '{"foo": "bar", "spam": 1}'
    const loadedValue = field.load(preloadedValue, context);
    expect(loadedValue).toStrictEqual(JSON.parse(preloadedValue));
});

test('JsonObjectField dumps from object', () => {
    const field = new JsonObjectField('data');
    const predumpedValue = {"foo": "bar", "spam": 1}
    const dumpedValue = field.dump(predumpedValue, context);
    expect(dumpedValue).toStrictEqual(JSON.stringify(predumpedValue));
});

test('SchemaField dumps through schema', () => {
    const field = new SchemaField('data', 'Dummy');
    const dumpedObject = field.dump({a: 1, b: 'foo'}, context);
    const dummy = new DummyMessage(1, 'foo');
    expect(dumpedObject).toStrictEqual(dummy);
});

test('SchemaField loads through schema', () => {
    const field = new SchemaField('data', 'Dummy');
    const dummy = new DummyMessage(1, 'foo');
    const loadedObject = field.load(dummy, context);
    expect(loadedObject).toStrictEqual({a: 1, b: 'foo'});
});

test('JsonSchema date-time int field parsed correctly', () => {
    const field = Field.fromJsonSchema('created_at', {
        'type': 'integer', 'format': 'date-time'
    });
    expect(field).toBeInstanceOf(DateTimeField);
});

test('JsonSchema date-time string field parsed correctly', () => {
    const field = Field.fromJsonSchema('created_at', {
        'type': 'string', 'format': 'date-time'
    });
    expect(field).toBeInstanceOf(DateTimeField);
});

test('JsonSchema list of strings field parsed correctly', () => {
    const field = Field.fromJsonSchema('strings', {
        'type': 'array', 'items': {'type': 'string'}
    });
    expect(field).toBeInstanceOf(ListField);
    expect(field.innerField).toBeInstanceOf(Field);
});

test('JsonSchema ref field parsed correctly', () => {
    const field = Field.fromJsonSchema('thing', {'$ref': 'Dummy'});
    expect(field).toBeInstanceOf(SchemaField);
});

test('JsonSchema object field parsed correctly', () => {
    const field = Field.fromJsonSchema('thing', {'type': 'object'})
    expect(field).toBeInstanceOf(JsonObjectField);
});

test('Schema correctly parses JsonSchema', () => {
    const schema = Schema.fromJsonSchema('Dummy', {
        'properties': {
            'a': {'type': 'integer'},
            'b': {'type': 'string'},
        }
    })
    expect(schema.name).toBe('Dummy')
    expect(schema.fields.a).toBeInstanceOf(Field);
    expect(schema.fields.b).toBeInstanceOf(Field);
});
