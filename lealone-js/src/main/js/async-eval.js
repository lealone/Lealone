
// 改编自: https://developer.mozilla.org/zh-CN/docs/Web/API/Web_Workers_API/Using_web_workers
var asyncEval = (function () {
  var aListeners = [], oParser = new Worker("data:text/javascript;charset=US-ASCII," +
        "onmessage%20%3D%20function%20%28oEvent%29%20%7B%0A%09postMessage%28%7B%0A%09%09%22id%22%3A%20oEvent.data.id%2C%0A%09%09%22evaluated%22%3A%20eval%28oEvent.data.code%29%0A%09%7D%29%3B%0A%7D");

  oParser.onmessage = function (oEvent) {
      if (aListeners[oEvent.data.id]) { aListeners[oEvent.data.id](oEvent.data.evaluated); }
      delete aListeners[oEvent.data.id];
  };

  return function (sCode, fListener) {
      sCode = `(function () { var task = ` + sCode.toString() + `
          function executeSqlSync(command) {
              var syncRequestUrl = "http://localhost:8080/_lealone_sync_request_";
              var xhr = new XMLHttpRequest();
              var formData = new FormData();
              formData.append('command', command);
              xhr.open('POST', syncRequestUrl, false);
              xhr.send(formData);
              return xhr.responseText;
          };
      ` + " return task();})()";
      aListeners.push(fListener || null);
      oParser.postMessage({
        "id": aListeners.length - 1,
        "code": sCode
      });
    };
})();
