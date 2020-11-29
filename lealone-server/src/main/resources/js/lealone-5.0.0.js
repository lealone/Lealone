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

L.getService = function(serviceName) {
    var object = {
        sockjsUrl: L.sockjsUrl,
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
        default:
            console.log("unknown response type: " + type + ", serviceName: " + serviceName + ", data: " + e.data)
        }
    };
    sockjs.onclose = function() {
        console.log("SockJS close");
    };
}

L.sockjsUrl = "/_lealone_sockjs_";
return {
    setSockjsUrl: function(url) { L.sockjsUrl = url },
    getService: L.getService
};
})();

