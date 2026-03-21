//function deepCopy(parent, child) {
//    child = child || {};
//    for(var i in parent) {
//        if(parent.hasOwnProperty(i)) {
//            if(typeof parent[i] === "object") {
//                child[i] = (Object.prototype.toString.call(parent[i]) === "[object Array]") ? [] : {};
//                deepCopy(parent[i], child[i]);
//            } else {
//                child[i] = parent[i];
//            }
//        }
//    }
//    return child;
//}

var deepCopy =  function(source) {
    var result;
    (source instanceof Array) ? (result = []) : (typeof(source)==="object")? (source===null?(result = ""):(result={})):(result=source);
    for (var key in source) {
        result[key] = (typeof source[key]==='object') ? this.deepCopy(source[key]) : source[key];
    }
    return result;
}