const messageClassModule = require('./app_pb.js');
const {
    GrpcApplicationClient
} = require('./app_grpc_web_pb.js');


class Schema {
    constructor(fields) {
        this.fields = fields;
    }

    load(message) {
        const result = {};
        for (const fieldName in this.fields) {
            const field = this.fields[fieldName];
            const rawValue = getFieldValue(message, field)
            if (rawValue !== null && rawValue !== undefined) {
                result[fieldName] = field.load(rawValue);
            }
        }
        return result;
    }

    dump(object, message) {
        for (const fieldName in this.fields) {
            const field = this.fields[fieldName];
            const rawValue = object[fieldName];
            if (rawValue !== null && rawValue !== undefined) {
                const setterName = `set${snakeToCamel(fieldName, true)}`;
                const dumpedValue = field.dump(rawValue);
                const setter = message[setterName].bind(message);
                setter(dumpedValue);
            }
        }
        return message;
    }
}

class Field {
    constructor(typeName, fieldName = null, many = false) {
        this.typeName = typeName;
        this.fieldName = fieldName;
        this.many = many;
        // TODO: handle inner types of list fields
    }

    onUnrecognized(value, opName) {
        const className = (typeof this).name;
        console.log(
            `unrecognized value ${value} while ${opName}ing ` +
            `gRPC field "${this.fieldName}" (type: ${this.typeName})`
        )
    }

    load(value) {
        return value;
    }

    dump(value) {
        return value;
    }
}

class ListField extends Field {
    constructor(typeName, fieldName, innerTypeName) {
        super(typeName, fieldName, true);
        this.innerTypeName = innerTypeName;
        this.innerField = null;

        switch (innerTypeName) {
            case 'Dict':
                this.innerField = new JsonObjectField(innerTypeName);
                break;
            case 'DateTime':
                this.innerField = new DateTimeField(innerTypeName);
                break;
            default:
                this.innerSchema = schemas[innerTypeName];
                if (this.innerSchema !== undefined) {
                    this.innerField = new ObjectField(innerTypeName);
                } else {
                    this.innerField = new Field(innerTypeName);
                }
                break;
        }
    }

    load(rawValues) {
        return rawValues.forEach((x) => this.innerField.load(x));
    }

    dump(value) {
        return rawValues.forEach((x) => this.innerField.dump(x));
    }
}

class ObjectField extends Field {
    load(value) {
        if (this.many) {
            return this.loadSingleObject(value);
        } else {
            return value.forEach(this.loadSingleObject);
        }
    }

    loadSingleObject(value) {
        const protoMessage = value;
        const schema = schemas[this.typeName];
        const jsonObject = {};

        for (const fieldName in schema.fields) {
            const field = schema.fields[fieldName];
            const getterName = `get${snakeToCamel(fieldName, true)}`;
            const fieldValue = protoMessage[getterName]();

            if (fieldValue !== null) {
                jsonObject[fieldName] = field.load(fieldValue);
            } else {
                jsonObject[fieldName] = fieldValue;
            }
        }
        return jsonObject;
    }

    dump(value) {}
}

class DateTimeField extends Field {
    load(value) {
        if (this.isNumber(value)) {
            // value should be a unix UTC timestamp (in seconds)
            return new Date(1000 * value)
        } else if (value instanceof String) {
            // value should be a datetime string
            return Date.parse(value);
        } else {
            this.onUnrecognized(value, "load");
            return value;
        }
    }

    dump(value) {
        if (value instanceof Date) {
            // generate a UTC unix timestamp
            return Math.floor((new Date()).getTime() / 1000);
        }
        if (this.isNumber(value)) {
            // input value expected in ms; must convert to seconds
            return value / 1000;
        } else {
            this.onUnrecognized(value, "dump");
            return value;
        }
    }

    isNumber(value) {
        return (typeof value === 'number' && isFinite(value));
    }
}

class JsonObjectField extends Field {
    load(value) {
        return JSON.parse(value);
    }
    dump(value) {
        return JSON.stringify(value);
    }
}

class GrpcService {

    constructor() {
    }

    static factory(scheme = 'http', host = 'localhost', port = 8080) {

        const serviceUrl = `${scheme}://${host}:${port}`;
        const grpcClient = new GrpcApplicationClient(serviceUrl);

        console.log(`gRPC client using address: ${serviceUrl}`);

        // create a new GrpcService subclass, specializing it
        // for the grpc client provided.
        class GrpcServiceSubclass extends GrpcService {}

        GrpcServiceSubclass.prototype.grpcClient = grpcClient;

        // bless the new subclass with public methods for client RPC methods.
        // note that we ignore "private" methods, which we define as ending or
        // starting with an underscore.
        for (let funcName in grpcClient) {
            if (!(funcName.startsWith('_') || funcName.endsWith('_'))) {
                GrpcServiceSubclass._buildGrpcClientRpcMethod(funcName);
            }
        }

        // return a singleton instance of the new class.
        return new GrpcServiceSubclass();
    }

    static _buildGrpcClientRpcMethod(clientFuncName) {
        const requestClassName = snakeToCamel(clientFuncName, true) + 'Request';
        const requestClass = messageClassModule[requestClassName];
        const responseClassName = snakeToCamel(clientFuncName, true) + 'Response';
        const responseClass = messageClassModule[responseClassName];

        this.prototype[snakeToCamel(clientFuncName)] = function(
            data = {}, metaData = {}, onResponse = null
        ) {
            const requestSchema = schemas[requestClassName];
            const request = requestSchema.dump(data, new requestClass());

            const send = this.grpcClient[clientFuncName].bind(this.grpcClient);
            send(request, metaData, (err, response) => {
                if (!err) {
                    const schema = schemas[responseClassName];
                    const responseData = schema.load(response);
                    onResponse(err, responseData);
                } else {
                    onResponse(err, response);
                }
            })
        };
    }

}


function snakeToCamel(snakeStr, capitalizeInitialChar = false) {
    var camelString = snakeStr.replace(
        /(_+[a-zA-Z0-9])/g, (group) => (
            group.toUpperCase().replace('-', '').replace('_', '')
        )
    )
    if (capitalizeInitialChar) {
        camelString = camelString.replace(/^\w/, c => c.toUpperCase());
    }
    return camelString;
}

function getFieldValue(message, field) {
    const getterName = `get${snakeToCamel(field.fieldName, true)}`;
    return message[getterName]();
}



const schemas = {
    {% for resource_type_name, resource_type in resource_types.items() %}
        {{ resource_type_name }}: new Schema({
        {% for resolver in resource_type.ravel.resolvers.values() %}
            {{ resolver.name }}:
            {%- if resolver.name in resource_type.ravel.resolvers.fields %}
                {%- if isinstance(resolver.field, field_types.DateTime) %}
                new DateTimeField('DateTime', "{{ resolver.name }}"),
                {%- elif isinstance(resolver.field, field_types.Nested) %}
                new Object('{{ get_stripped_schema_name(resolver.field.schema) }}', "{{ resolver.name }}"),
                {%- elif isinstance(resolver.field, schema_type) %}
                new ObjectField('{{ get_stripped_schema_name(resolver.field) }}', "{{ resolver.name }}"),
                {%- elif isinstance(resolver.field, field_types.Dict) %}
                new JsonObjectField('Dict', "{{ resolver.name }}"),
                {%- else %}
                new Field('{{ get_class_name(resolver.field) }}', "{{ resolver.name }}"),
                {%- endif %} 
            {% elif resolver.target %}
                new ObjectField('{{ get_class_name(resolver.target) }}', "{{ resolver.name }}"),
            {% endif %}
        {%- endfor %}
        }),
    {% endfor %}

{% for action in app.actions.values() %}
    {% for schema in action.schemas.values() %}
        {{ get_stripped_schema_name(schema) }}: new Schema({
        {% for field in schema.fields.values() %}
            {{ field.name }}:
            {%- if isinstance(field, field_types.DateTime) %}
            new DateTimeField('DateTime', "{{ field.name }}"),
            {%- elif isinstance(field, field_types.Nested) %}
            new Object('{{ get_stripped_schema_name(field.schema) }}', "{{ field.name }}"),
            {%- elif isinstance(field, schema_type) %}
            new ObjectField('{{ get_stripped_schema_name(field) }}', "{{ field.name }}"),
            {%- elif isinstance(field, field_types.Dict) %}
            new JsonObjectField('Dict', "{{ field.name }}"),
            {%- else %}
            new Field('{{ get_class_name(field) }}', "{{ field.name }}"),
            {%- endif %} 
        {% endfor %}
        }),
    {% endfor %}
{% endfor %}
}


function test() {
    service = GrpcService.factory();
    const request = {'data': {'name': 'Test Product'}};
    for (var i = 0; i < 50; i++) {
        service.customerStartsProductCreationWizard(request, {}, (err, response) => {
            //console.log(response);
        });
    }
}

test();
