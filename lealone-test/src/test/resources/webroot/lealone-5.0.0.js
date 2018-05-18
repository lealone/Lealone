var lealone = (function() {
var L = {};
L.call = function(object, apiName) {
	if(!L.sockjs) {
		L.services = {};
		initSockJS(object.sockjsUrl);
	}
	var serviceName = object.serviceName + "." + apiName;
	//格式: type;serviceName;[arg1,arg2,...argn]
    var msg = "1;" + serviceName;
	var length = arguments.length;
	if(typeof arguments[length - 1] == 'function') {
		L.services[serviceName] = function() {};
        L.services[serviceName]["callback"] = arguments[length - 1];
        length--;
	}
	if(length > 2) {
		msg += ";[";
		for(var j = 2; j < length; j++) {
			if(j != 2) {
				msg += ",";
			}
			msg += JSON.stringify(arguments[j]);
		}
		msg += "]";
	}
	if(L.sockjsReady)
	    L.sockjs.send(msg);
	else {
		if(!L.penddingMsgs) {
			L.penddingMsgs = [];
		} 
		L.penddingMsgs.push(msg);
	}
};

var proxyObject = function (object, missingMethod) {
	  const proxyObject = new Proxy(object, {
	    get(object, property) {
	      if (Reflect.has(object, property)) {
	        return Reflect.get(object, property);
	      } else {
	        return (...args) => Reflect.apply(missingMethod, proxyObject, [object, property, ...args]);
	      }
	    }
	  });
	  return proxyObject;
};

var missingMethod  = function(object, method, ...args) {
	L.call(object, method, ...args);
};

L.getService = function(sockjsUrl, serviceName) {
	var object = {
		sockjsUrl: sockjsUrl,
		serviceName: serviceName
	}
	return proxyObject(object, missingMethod);
};

function initSockJS(sockjsUrl) {
    //var sockjs = new SockJS(sockjsUrl, {"transports":"xhr_streaming"});
    var sockjs = new SockJS(sockjsUrl);
	L.sockjs = sockjs;
    sockjs.onopen = function() {
		L.sockjsReady = true; 
		if(L.penddingMsgs) {
			for(var i = 0; i < L.penddingMsgs.length; i++) {
				sockjs.send(L.penddingMsgs[i]);
			}
			L.penddingMsgs = [];
		}
    };
    sockjs.onmessage = function(e) {
		var a = JSON.parse(e.data);
    	var type = a[0];
    	var serviceName = a[1]; 
    	var result = a[2];
    	switch(type) {
		case 2: // 正常返回
			//如果有回调就执行它
			if(L.services[serviceName] && L.services[serviceName]["callback"]) { 
			    L.services[serviceName]["callback"](result);
			}
    		break;
		case 3: // error info
			console.log("failed to call service: " + serviceName + ", backend error: " + result)
    		break;
		case 500:
		case 501:
		case 502:
		case 503:
			if(L.sqls[a[1]] && L.sqls[a[1]]["callback"]) { 
			    L.sqls[a[1]]["callback"](result);
			}
			break;
    	default:
    		console.log("unknown response type: " + type + ", serviceName: " + serviceName + ", data: " + e.data)
    	}
    };
    sockjs.onclose = function() {
    	console.log("SockJS close");
    };
}

var id = 0;
L.executeSql = function(type, sql, args, callback) {
	id++;
	if(!L.sockjs) {
		L.sqls = {};
		initSockJS(L.sockjsUrl);
	}
    var msg = type + ";" + id + ";" + sql; 
	if(typeof callback == 'function') {
		L.sqls[id] = function() {};
        L.sqls[id]["callback"] = callback; 
	}
	if(args) {
		msg += ";[";
		for(var j = 0; j < args.length; j++) {
			if(j != 0) {
				msg += ",";
			}
			msg += JSON.stringify(args[j]);
		}
		msg += "]";
	}
	if(L.sockjsReady)
	    L.sockjs.send(msg);
	else {
		if(!L.penddingMsgs) {
			L.penddingMsgs = [];
		} 
		L.penddingMsgs.push(msg);
	}
};
L.sockjsUrl = "/_lealone_sockjs_";
return {
	setSockjsUrl: function(url) { L.sockjsUrl = url },
	getService: L.getService, 
	executeSql: L.executeSql
};
})();

const REGULAR_MODEL = 0;
const ROOT_DAO = 1;
const CHILD_DAO = 2;

class ArrayStack  {
	constructor() {
		this.list = new Array();
	} 
    push(item) {
    	this.list.push(item);
    }
 
    pop() {
        return this.list.pop();
    }

    peek() {
    	var len = this.list.length;
        if (len == 0) {
        	throw new RangeError("Array stack is empty");
        }
        return this.list[len - 1];
    }
}

class Model {
    constructor(modelTable, modelType) {
    	this.modelTable = modelTable;
    	this.modelType = modelType;
    	this.reset();
    }
    
    reset() {
    	this.modelProperties = [];
    	this.expressionBuilderStack = null;
    	this.whereExpressionBuilder = null;
    	this.nvPairs = null;
    }
    
    addNVPair(name, value) {
        if (this.nvPairs == null) {
        	this.nvPairs = new Map();
        }
        this.nvPairs.set(name, value);
    }
    
    setModelProperties(modelProperties) {
        this.modelProperties = modelProperties;
    }

    where() {
        return this;
    }
    
    checkDao(methodName) {
        if (!this.isDao()) {
            throw new TypeError("The " + methodName + " operation is not allowed, please use "
                    + this.constructor.name + ".dao." + methodName + "() instead.");
        }
    }

    isDao() {
        return this.modelType > 0;
    }
    
    findOne(cb) {
    	this.checkDao("findOne");
    	var args = [];
        var sql = "select * from " + this.modelTable.tableName; 
        if (this.whereExpressionBuilder != null) {
            sql += " where " + this.whereExpressionBuilder.getExpression();
            args = this.whereExpressionBuilder.values;
        }
        console.log("execute sql: " + sql);
        this.reset();
        lealone.executeSql(503, sql, args, cb)
    }
    
    insert(cb) {
    	// TODO 是否允许通过 XXX.dao来insert记录?
        if (this.isDao()) {
            var name = this.constructor.name;
            throw new TypeError("The insert operation is not allowed for " + name
                    + ".dao,  please use new " + name + "().insert() instead.");
        }
        if(this.nvPairs == null) {
        	return 0;
        }
        var sql = "insert into " + this.modelTable.tableName + " ("; 
        var sqlValues = ") values (";
        var i = 0;
        var args = [];
        this.nvPairs.forEach(function(value, key) {
        	if(i != 0) {
        		sql += ", ";
        		sqlValues += ", ";
        	}
        	sql += key;
        	sqlValues += "?";
        	args.push(value);
        	i++;
        })
        sql += sqlValues + ")";
        console.log("execute sql: " + sql);
        this.reset();
        lealone.executeSql(500, sql, args, cb)
        return 0;
    }
    
    update(cb) {
    	var sql = "update " + this.modelTable.tableName + " set "; 
        var i = 0;
        var args = [];
        this.nvPairs.forEach(function(value, key) {
        	if(i != 0) {
        		sql += ", ";
        	}
        	sql += key + " = ?";
        	args.push(value);
        	i++;
        })
        if (this.whereExpressionBuilder != null) {
    		sql += " where " + this.whereExpressionBuilder.getExpression();
    		args = args.concat(this.whereExpressionBuilder.values);
        }
    	console.log("execute sql: " + sql);
    	this.reset();
    	lealone.executeSql(501, sql, args, cb)
        return 0;
    }
    
    delete(cb) {
    	var sql = "delete from " + this.modelTable.tableName + " "; 
    	var args = [];
    	if (this.whereExpressionBuilder != null) {
    		sql += " where " + this.whereExpressionBuilder.getExpression();
    		args = this.whereExpressionBuilder.values;
    	}
    	console.log("execute sql: " + sql);
    	this.reset();
    	lealone.executeSql(502, sql, args, cb)
        return 0;
    }
    
    peekExprBuilder() {
    	return this.getStack().peek();
    }
    
    getStack() {
        if (this.expressionBuilderStack == null) {
        	this.expressionBuilderStack = new ArrayStack();
        	this.expressionBuilderStack.push(this.getWhereExpressionBuilder());
        }
        return this.expressionBuilderStack;
    }
    
    getWhereExpressionBuilder() {
        if (this.whereExpressionBuilder == null) {
        	this.whereExpressionBuilder = new ExpressionBuilder(this);
        }
        return this.whereExpressionBuilder;
    }
}

class ModelTable {
    constructor(databaseName, schemaName, tableName) {
    	this.databaseName = databaseName;
    	this.schemaName = schemaName;
    	this.tableName = tableName;
    }
}

class ModelProperty {
    constructor(name, model) {
    	this.name = name;
    	this.value = null;
    	this.model = model;
    }
    
    get() {
        return this.value;
    }

    set(newValue) {
    	if (this.value != newValue) {
            this.value = newValue;
            this.expr().set(this.name, newValue); 
        }
        return this.model;
    }
    
    eq(value) {
    	this.expr().eq(this.name, value);
        return this.model;
    }
    
    getFullName() {
    	return this.name;
    }
    
    expr() {
        return this.model.peekExprBuilder();
    }
}
class PString extends ModelProperty {
    constructor(name, model) {
    	super(name, model);
    }

//    set(newValue) {
//    	if (this.value != newValue) {
//            this.value = newValue;
//            this.expr().set(this.name, "'" + newValue + "'"); 
//        }
//        return this.model;
//    }
}
class PInteger extends ModelProperty {
    constructor(name, model) {
    	super(name, model);
    }
}
class PLong extends ModelProperty {
    constructor(name, model) {
    	super(name, model);
    }
}

class ExpressionBuilder {
    constructor(model) {
    	this.model = model;
    	this.isAnd = true;
    	this.expression = null;
    	this.orderList = [];
    	this.values = [];
    }
    setAnd(isAnd) {
        this.isAnd = isAnd;
    }

    // 用于join时切换
    setModel(model) {
        this.model = model;
    }

    getExpression() {
        return this.expression;
    }

    junction(expressionBuilder) {
    	this.setRootExpression(this.expressionBuilder.getExpression());
        return this;
    }

    getOrderList() {
        return this.orderList;
    }

    getTable() {
        return this.model.getTable();
    }

    setRootExpression(e) {
        if (this.expression == null) {
        	this.expression = e;
        } else {
        	if(this.isAnd) {
        		this.expression = this.expression + " and " + e;
        	} else {
        		this.expression = this.expression + " or " + e;
        	}
        }
    }

    addExpression(propertyName, value, compareType) {
    	var e = propertyName + " " + compareType + " ?"; 
        this.setRootExpression(e);
        this.values.push(value);
    }

    set(propertyName, value) {
    	this.model.addNVPair(propertyName, value);
        return this;
    }

    eq(propertyName, value) {
    	if(value instanceof ModelProperty) {
    		this.setRootExpression(propertyName + " = " + modelProperty.getFullName());
    	} else {
    		this.addExpression(propertyName, value, "=");
    	}
        return this;
    }

    ne(propertyName, value) {
    	this.addExpression(propertyName, value, "!=");
        return this;
    }

    ieq(propertyName, value) {
        this.eq(propertyName, value); // TODO
        return this;
    }

    between(propertyName, value1, value2) {
    	var e = "(" + propertyName + " between ? and ?)"; 
        this.setRootExpression(e);
        this.values.push(value1);
        this.values.push(value2);
        return this;
    }

    gt(propertyName, value) {
    	this.addExpression(propertyName, value, ">");;
        return this;
    }

    ge(propertyName, value) {
    	this.addExpression(propertyName, value, ">=");;
        return this;
    }

    lt(propertyName, value) {
    	this.addExpression(propertyName, value, "<");;
        return this;
    }

    le(propertyName, value) {
    	this.addExpression(propertyName, value, "<=");;
        return this;
    }

    isNull(propertyName) {
    	var e = "(" + propertyName + " is null)"; 
        this.setRootExpression(e);
        return this;
    }

    isNotNull(propertyName) {
    	var e = "(" + propertyName + " is not null)"; 
        this.setRootExpression(e);
        return this;
    }

    arrayContains(propertyName, values) {
        // TODO Auto-generated method stub
        return this;
    }

    arrayNotContains(propertyName, values) {
        // TODO Auto-generated method stub
        return this;
    }

    arrayIsEmpty(propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    arrayIsNotEmpty(propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    in(propertyName, values) {
        // TODO Auto-generated method stub
        return this;
    }

    notIn(propertyName, values) {
        // TODO Auto-generated method stub
        return this;
    }

    like(propertyName, value) {
    	var e = "(" + propertyName + " like ?)"; 
        this.values.push(value);
        return this;
    }

    ilike(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    startsWith(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    istartsWith(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    endsWith(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    iendsWith(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    contains(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    icontains(propertyName, value) {
        // TODO Auto-generated method stub
        return this;
    }

    match(propertyName, search) {
        // TODO Auto-generated method stub
        return this;
    }

    and() {
    	this.isAnd = true;
        return this;
    }

    or() {
    	this.isAnd = false;
        return this;
    }

    not() {
        // TODO Auto-generated method stub
        return null;
    }

    orderBy(propertyName, isDesc) {
        return this;
    }

}

