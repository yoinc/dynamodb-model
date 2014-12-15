var async = require('async');
var DynamoQuery = require('./query');

function mapToArray(source, fn) {
    var result = [];
    for (var key in source) {
        result.push(fn(key, source[key], source));
    }
    return result;
}

function mapToObject(source, fn) {
    var result = {};
    for (var key in source) {
        var r = fn(key, source[key], source);
        for (var resultKey in r) {
            result[resultKey] = r[resultKey];
        }
    }
    return result;
}

function pairs(source) {
    var result = [];
    for (var key in source) {
        result.push([key, source[key]]);
    }
    return result;
}

function DynamoModel (tableName, schema, dynamodb) {
    this.tableName = tableName;
    this.schema = schema;

    this.tableStatus = 'undefined';
    this.tableStatusPendingCallbacks = [];

    if (!tableName) throw new Error('tableName is required');
    if (!schema) throw new Error('schema is required');

    this.dynamodb = dynamodb;
    this.consistentRead = false;
    this.defaultThroughput = {
        ReadCapacityUnits: options.ReadCapacityUnits ? options.ReadCapacityUnits : 10,
        WriteCapacityUnits: options.WriteCapacityUnits ? options.WriteCapacityUnits : 5
    };

    // make sure the table is available as soon as possible
    if (options.skipWaitForActiveTable === true) return;

    this.waitForActiveTable();
}

// a default callback making sure errors are not lost
DynamoModel.prototype.defaultCallback = function (err) {
    if (err) throw err;
};

DynamoModel.prototype.getItem = function (key, options, callback) {
    if (!key) throw new Error('key is required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    callback = callback || this.defaultCallback;

    var params = options || {};
    params.TableName = this.tableName;
    params.Key = this.schema.mapToDb(key);
    if (this.consistentRead) params.ConsistentRead = true;

    var instance = this;

    this.waitForActiveTable(function (err) {
        if (err) return callback(err);
        return instance.dynamodb.getItem(params, function (err, response) {
            if (err) return callback(err);
            return callback(null, instance.schema.mapFromDb(response.Item), response);
        });
    });
};

DynamoModel.prototype.batchGetItems = function (keys, options, callback) {
    if (!keys) throw new Error('keys are required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    callback = callback || this.defaultCallback;

    var instance = this;

    var params = options || {};
    params.RequestItems = {};
    params.RequestItems[this.tableName] = { Keys: keys.map( function(item) { return instance.schema.mapToDb(item); } ) };

    if (this.consistentRead) {
      params.RequestItems[this.tableName].ConsistentRead = true;
    } else if (params.ConsistentRead) {
      delete params.ConsistentRead;
      params.RequestItems[this.tableName].ConsistentRead = true;
    }

    this.waitForActiveTable(function (err) {
        if (err) return callback(err);
        return instance.dynamodb.batchGetItem(params, function (err, data, response) {
            if (err) return callback(err);

            if (data.Responses[instance.tableName].length === 0) return callback(null, null, response);

            var results = data.Responses[instance.tableName].map( function(item) {
              return instance.schema.mapFromDb(item);
            });

            return callback(null, results, response);
        });
    });
};

DynamoModel.prototype.putItem = function (item, options, callback) {
    if (!item) throw new Error('item is required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    callback = callback || this.defaultCallback;

    var params = options || {};
    params.TableName = this.tableName;
    params.Item = this.schema.mapToDb(item);

    var instance = this;

    this.waitForActiveTable(function (err) {
        if (err) return callback(err);
        instance.dynamodb.putItem(params, function (err, response) {
            if (err) return callback(err);
            callback(null, instance.schema.mapFromDb(response.Attributes), response);
        });
    });
};

DynamoModel.prototype.updateItem = function (key, updates, options, callback) {
    if (!key) throw new Error('key is required');
    if (!updates) throw new Error('updates is required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    callback = callback || this.defaultCallback;

    var params = options || {};
    params.TableName = this.tableName;
    params.Key = this.schema.mapToDb(key);
    params.AttributeUpdates = this.parseUpdates(updates);

    var instance = this;

    this.waitForActiveTable(function (err) {
        if (err) return callback(err);
        instance.dynamodb.updateItem(params, callback);
    });
};

DynamoModel.prototype.deleteItem = function (key, options, callback) {
    if (!key) throw new Error('key is required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }
    callback = callback || this.defaultCallback;

    var params = options || {};
    params.TableName = this.tableName;
    params.Key = this.schema.mapToDb(key);

    var instance = this;

    this.waitForActiveTable(function (err) {
        if (err) return callback(err);
        instance.dynamodb.deleteItem(params, callback);
    });
};

DynamoModel.prototype.query = function (key, options, callback) {
    if (!key) throw new Error('key is required');
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    var params = options || {};
    params.TableName = this.tableName;
    params.KeyConditions = this.parseConditions(key);
    if (this.consistentRead) params.ConsistentRead = true;

    var query = new DynamoQuery('query', this.schema, this, params);
    if (callback) return query.exec(callback);
    return query;
};

DynamoModel.prototype.scan = function (filter, options, callback) {
    if (typeof filter === 'function') {
        callback = filter;
        options = {};
        filter = null;
    }
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    var params = options || {};
    params.TableName = this.tableName;
    if (filter) {
        params.ScanFilter = this.parseConditions(filter);
    }

    var query = new DynamoQuery('scan', this.schema, this, params);
    if (callback) return query.exec(callback);
    return query;
};

DynamoModel.prototype.describeTable = function (options, callback) {
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    var params = options || {};
    params.TableName = this.tableName;
    return this.dynamodb.describeTable(params, callback);
};

DynamoModel.prototype.createTable = function(options, callback) {
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    var params = options || {};
    params.TableName = this.tableName;
    params.KeySchema = mapToArray(this.schema.keys, function (name, val) {
        return { AttributeName: name, KeyType: val };
    });
    var schema = this.schema;
    params.AttributeDefinitions = mapToArray(this.schema.keys, function (name, val) {
        return { AttributeName: name, AttributeType: schema.attributes[name] };
    });
    params.ProvisionedThroughput = params.ProvisionedThroughput || this.defaultThroughput;

    return this.dynamodb.createTable(params, callback);
};

DynamoModel.prototype.updateTable = function (options, callback) {
    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    var params = options || {};
    params.TableName = this.tableName;
    params.ProvisionedThroughput = params.ProvisionedThroughput || this.defaultThroughput;
    return this.dynamodb.updateTable(params, callback);
};

DynamoModel.prototype.deleteTable = function (options, callback) {
    var params = options || {};
    params.TableName = this.tableName;
    return this.dynamodb.deleteTable(params, callback);
};

DynamoModel.prototype.waitForActiveTable = function (pollingTimeout, callback) {
    if (typeof pollingTimeout === 'function') {
        callback = pollingTimeout;
        pollingTimeout = null;
    }
    pollingTimeout = pollingTimeout || 3000;

    var instance = this;

    if (this.tableStatus === 'undefined') {
        this.tableStatus = 'querying';
        if (callback) {
            this.tableStatusPendingCallbacks.push(callback);
        }
        async.waterfall([
            function getTableDescription(callback) {
                instance.describeTable(function (err, response) {
                    if (err) {
                        if (err.code === 'ResourceNotFoundException') {
                            // table does not exist, create it now
                            instance.createTable(function (err, response) {
                                if (err) return callback(err);
                                callback(null, response.TableDescription);
                            });
                        } else {
                            return callback(err);
                        }
                    } else {
                       callback(null, response.Table);
                    }
                });
            },
            function pollUntilActive(tableDescription, callback) {
                async.until(function () {
                    return tableDescription.TableStatus === 'ACTIVE';
                }, function (callback) {
                    setTimeout(function () {
                        instance.describeTable(function (err, response) {
                            if (err) return callback(err);
                            tableDescription = response.Table;
                            callback();
                        });
                    }, pollingTimeout);
                }, function (err) {
                    if (err) return callback(err);
                    callback(null, tableDescription);
                });
            }
        ], function (err, response) {
            // save results for future calls
            instance.tableStatus = { error: err, response: response };
            // fire all pending callbacks
            for (var i = 0; i < instance.tableStatusPendingCallbacks.length; i++) {
                instance.tableStatusPendingCallbacks[i](err, response);
            }
            instance.tableStatusPendingCallbacks = [];
        });
    } else if (instance.tableStatus === 'querying' && callback) {
        // already querying table status, add callback to queue
        instance.tableStatusPendingCallbacks.push(callback);
    } else {
        return callback(instance.tableStatus.error, instance.tableStatus.response);
    }
};

DynamoModel.prototype.parseConditions = function (conditions) {
    var instance = this;

    var result = {};
    pairs(conditions).forEach(function (pair) {

        var key = pair[0];
        var value = pair[1];
        var operator = 'EQ';

        if (typeof value === 'object') {
            var val = pairs(value);
            if (val.length === 1) {
                operator = val[0][0];
                value = val[0][1];
            }
        }

        if (operator === '$gt') {
            operator = 'GT';
        } else if (operator === '$gte') {
            operator = 'GE';
        } else if (operator === '$lt') {
            operator = 'LT';
        } else if (operator === '$lte') {
            operator = 'LE';
        } else if (operator === '$begins') {
            operator = 'BEGINS_WITH';
        } else if (operator === '$between') {
            operator = 'BETWEEN';
        } else if (/^\$/.test(operator)) {
            throw new Error('conditional operator "' + operator + '" is not supported');
        }

        var values = [];

        if (operator === 'BETWEEN') {
            if (!(Array.isArray(value) && value.length === 2)) {
                throw new Error('BETWEEN operator must have an array of two elements as the comparison value');
            }
            values.push(instance.schema.mappers[key].mapToDb(value[0]));
            values.push(instance.schema.mappers[key].mapToDb(value[1]));
        } else if (Array.isArray(value)) {
            throw new Error('this operator does not support array values');
        } else {
            values.push(instance.schema.mappers[key].mapToDb(value));
        }

        result[key] = {
            AttributeValueList: values,
            ComparisonOperator: operator
        };

    });
    return result;
};

DynamoModel.prototype.parseUpdates = function (updates) {
    var instance = this;

    return mapToObject(updates, function (key, value) {

        var result = {};
        var mapper;

        // look for a MongoDB-like operator and translate to its DynamoDB equivalent
        if (/^\$/.test(key)) {
            var action;
            if (key === '$set') action = 'PUT';
            if (key === '$unset') action = 'DELETE';
            if (key === '$inc') action = 'ADD';
            if (!action) {
                throw new Error('update operator "' + key + '" is not supported');
            }
            pairs(value).forEach(function (pair) {
                if (!instance.schema.mappers.hasOwnProperty(pair[0])) {
                    throw new Error('unknown field: ' + pair[0]);
                }
                mapper = instance.schema.mappers[pair[0]];
                result[pair[0]] = {
                    Action: action,
                    Value: mapper.mapToDb(pair[1])
                };
            });
            return result;
        }

        if (!instance.schema.mappers.hasOwnProperty(key)) {
            throw new Error('unknown field: ' + key);
        }
        mapper = instance.schema.mappers[key];

        if (typeof value === 'object') {
            var val = pairs(value);
            if (val.length === 1) {
                result[key] = {
                    Action: val[0][0],
                    Value: mapper.mapToDb(val[0][1])
                };
            } else {
                throw new Error('value is expected to contain only one key');
            }
        } else {
            result[key] = {
                Action: 'PUT',
                Value: instance.schema.mappers[key].mapToDb(value)
            };
        }

        return result;
    });
};

module.exports = DynamoModel;
