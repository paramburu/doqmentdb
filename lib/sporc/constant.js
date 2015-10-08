'use strict';

/**
 * @expose
 */
module.exports = {
  $UPDATE: {
    id: 'findAndModify',
    body: update
  },
  $FIND_OR_CREATE: {
    id: 'findOrCreate',
    body: findOrCreate
  },
  $REMOVE: {
    id: 'findAndRemove',
    body: remove
  },
  $BULK_CREATE: {
    id: 'bulkCreate',
    body: bulkCreate
  }
};

/**
 * @description
 * update/findAndModify stored procedure.
 * @example
 *
 *    update('SELECT * from root r', { active: true })
 *    update('SELECT * from root r WHERE r.name = "bar"', { name: 'foo' })
 *    update('SELECT * from root r WHERE inUDF(r.arr, 3)', { arr: { $concat: [4, 5] } })
 *
 * @param query
 * @param object
 * @param one     - indicate if to update only the first one
 */
function update(query, object, one) {
  var context = getContext()
    , manager = context.getCollection();

  // This function comes from utils.extend
  function extend(dst, obj) {
    var _keys = Object.keys(obj);
    for (var j = 0, jj = _keys.length; j < jj; j++) {
      var key = _keys[j]
        , val = obj[key];
      // maybe operation
      if(!Array.isArray(val) && typeof val == 'object') {
        var fKey = Object.keys(val)[0]
          , op = fKey.substr(1);
        // it's an operation
        if(fKey[0] == '$' && dst[key][op]) {
          var args =  Array.isArray(val[fKey]) ? val[fKey] : [val[fKey]]
            , res = dst[key][op].apply(dst[key], args);
          // e.g: [].pop/push/shift/...
          dst[key] = typeof dst[key] == 'object' && res.constructor != dst[key].constructor
            ? dst[key]
            : res;
          // if it's nested object
        } else if(typeof dst[key] == 'object' && dst[key] != null) {
          dst[key] = extend(dst[key], val);
        }
      } else dst[key] = val;
    }
    return dst;
  }

  // Query Documents
  manager.queryDocuments(manager.getSelfLink(), query, function(err, docs) {
    if(err) throw new Error(err.message);

    docs = docs.slice(0, one ? 1 : docs.length);
    // If it's wrap with promise
    object = object.fulfillmentValue || object;

    // Extend operation
    docs.forEach(function(doc) {
      doc = extend(doc, object);
      manager.replaceDocument(doc._self, doc);
    });

    // Set response body
    context.getResponse().setBody(one ? (docs[0] || typeof docs[0]) : docs);
  });
}



/**
 * @description
 * bulkCreate stored procedure.
 * @example
 *
 *    bulkCreate([{ name: 'foo' }, { name: 'bar' }, { itemId: 2 }])
 *
 * @param docs
 */
function bulkCreate(docs) {
  var context = getContext()
    , manager = context.getCollection()
    , _self = manager.getSelfLink()
    , count = 0  // The count of imported docs, also used as current doc index.
    ;

  // Validate input.
  if (!docs) throw new Error("The array is undefined or null.");

  var docsLength = docs.length;
  if (docsLength === 0) {
    context.getResponse().setBody(0);
    return;
  }

  // Call the CRUD API to create a document.
  tryCreate(docs[count], callback);

  // Note that there are 2 exit conditions:
  // 1) The createDocument request was not accepted.
  //    In this case the callback will not be called, we just call setBody and we are done.
  // 2) The callback was called docs.length times.
  //    In this case all documents were created and we don't need to call tryCreate anymore. Just call setBody and we are done.
  function tryCreate(doc, callback) {
    var isAccepted = manager.createDocument(_self, doc, callback);

    // If the request was accepted, callback will be called.
    // Otherwise report current count back to the client,
    // which will call the script again with remaining set of docs.
    // This condition will happen when this stored procedure has been running too long
    // and is about to get cancelled by the server. This will allow the calling client
    // to resume this batch from the point we got to before isAccepted was set to false
    if (!isAccepted) context.getResponse().setBody(count);
  }

  // This is called when collection.createDocument is done and the document has been persisted.
  function callback(err, doc, options) {
    if (err) throw err;

    // One more document has been inserted, increment the count.
    count++;

    if (count >= docsLength) {
      // If we have created all documents, we are done. Just set the response.
      context.getResponse().setBody(count);
    } else {
      // Create next document.
      tryCreate(docs[count], callback);
    }
  }
}



/**
 * @description
 * findOrCreated stored procedure.
 * @example
 *
 *    findOrCreate('SELECT * from root r WHERE r.name = "foo"', { name: 'foo' })
 *
 * @param query
 * @param object
 */
function findOrCreate(query, object) {
  var context = getContext()
    , manager = context.getCollection()
    , _self = manager.getSelfLink();

  // Query Documents
  manager.queryDocuments(_self, query, function(err, docs) {
    if(err) throw new Error(err.message);

    // If it exist, return the first result
    if(docs.length) {
      return context.getResponse().setBody(docs[0]);
    }
    manager.createDocument(_self, object, function(err, data) {
      if(err) throw new Error(err.message);
      return context.getResponse().setBody(data);
    });
  });
}


/**
 * @description
 * find[One]AndRemove stored procedure.
 * @example
 *
 *    findAndRemove('SELECT * from root r WHERE r.name = "foo"', true)
 *    findAndRemove('SELECT * from root r)
 *
 * @param query - query builder result
 * @param one   - indicate if to remove only the first one
 */
function remove(query, one) {
  var context = getContext()
    , manager = context.getCollection();

  // Query Documents
  manager.queryDocuments(manager.getSelfLink(), query, function(err, data) {
    if(err) throw new Error(err.message);

    // Response body
    var body = [];
    if(data.length) {
      var itr = data.slice(0, one ? 1 : data.length);
      itr.forEach(function(doc) {
        manager.deleteDocument(doc._self, push);
        function push(err, data) { body.push(data); }
      });
    }
    return context.getResponse().setBody(body);
  });
}