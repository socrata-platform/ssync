// Create a DatasyncUpload object like this:
//
//   var ds = new DatasyncUpload(params)
//
// params is an (optional) object that may contain:
//   forceWorkerReload : boolean -- append a cache-busting query parameter to workerUrl (default false)
//   log : function to log an object (default do not log)
//   workerUrl : string -- path to Javascript containing SSync logic (default implementation-dependent)
//
// There should be no reason to create more than one DatasyncUpload.  It can arbitrate access
// to its worker thread among multiple users.
//
// A DatayncUpload object has a single method, upload, with the following parameters:
//      ds.upload(datasetId, controlFile, file, params)
// datasetId   - the 4x4 of the dataset to be updated
// controlFile - the job descriptor; it must be something that can be serialized with JSON.stringify
// file        - the File object to upload
// params      - optional, and if provided must be an object with any or all of the following fields:
//   progress   - a callback which receives a progress object (see below)
//   onError    - a callback which receives error details (see below)
//   onComplete - a callback which receives a progress object
//   headers    - a dictionary of headers to be attached to all HTTP requests made
//                    (for auth, CSRF, app token, etc)
//
// A progress object, as passed to the progress and onComplete
// callbacks, looks like the following:
//   {
//     stage            : a value which names a processing stage (see below)
//     file_bytes_read  : the number of bytes read from the file
//     file_bytes_total : the total number of bytes in the file (== file.size)
//     bytes_uploaded   : the number of bytes uploaded so far
//     status           : if defined, a (parsed) JSON object representing the most recent processing status
//     log              : if defined, a (parsed) JSON array representing the processing log for the current job
//   }
// The last two are only available when the stage has reached WatchingProgress.
//
// Processing proceeds through the following stages.  These names are available as values on the
// datasync upload object:
//    RetrievingSignature : making the inital http GETs to find a preexisting signature
//    ParsingSignature    : extracting the retrieved signature
//    UploadingData       : reading the file, chunking it, diffing it against the signature, and uploading
//    UploadingCommit     : sending the control file to the server
//    WatchingProgress    : observing the status and log files for the committed job
//
// When the status arrives at a value with type 'success' or 'failure', processing is finished
// and onComplete is called with the final progress information.  Note an upstream failure does
// NOT cause onError to be called!
//
// If an error happens before processing completes, the onError callback is invoked with an object
// with fields 'type' and 'details'; the former is a tag for the error type, and the latter is
// a tag-specific object.  The tags that may occur are the following:
//    AuthenticationFailed     : an HTTP request failed wih a 401 response, or a 403 response and it looked like a core server "auth required" response
//          details: /* JSON response received */
//    ChunkUploadFailed        : A network or HTTP error occurred while uploading a chunk of data
//          details: { status: HTTP status } // TODO non-HTTP errors
//    ChunkUploadReturnedInvalidResponse : The upload of a chunk did not result in an object with a 'blobId' field
//          details: { response: actual response received }
//    ChunkUploadMalformed     : The upload of a chunk returned non-json
//          details: { /* none */ }
//    CommitFailed             : A network or HTTP error occurred while uploading the control file
//          details: { status: HTTP status } // TODO non-HTTP errors
//    CommitReturnedInvalidResponse : The commit file upload did not return an object with a 'jobId' field
//          details: { response: actual response received }
//    CommitMalformed          : The commit request returned non-json
//          details: { /* None */ }
//    Forbidden                : an HTTP request failed with a 403
//          details: /* JSON response received */
//    InternalError            : An internal invariant was violated; this is always a bug in this code.
//          details: { /* none */ }
//    OptimumBlockSizeRetrievalFailed : The request to the server to get the upload block size failed
//          details: { status: HTTP status } // TODO non-HTTP errors
//    OptimumBlockSizeRetrievalMalformed : The request to the server to get the upload block size returned non-JSON
//          details: { /* none */ }
//    SignatureParseFailed     : The signature file was corrupt
//          details: currently in flux, but likely not interesting to an end-user anyway
//    SignatureRetrievalFailed : A network or HTTP error occurred while retrieiving the signature file
//          details: { status: HTTP status } // TODO non-HTTP errors
//    SignatureRetrieivalMalformed : One of the requests to find the signature returned non-JSON
//          details: { /* none */ }
//    WatchFailed              : A network or HTTP erro occurred while watching the upload progress.
//          details: { status: HTTP status } // TODO non-HTTP errors
//    WatchMalformed           : A watch request returned non-JSON
//          details: { /* none */ }

var DatasyncUpload = (function ($) {
    // setTimeout has a minimum delay; this uses postMessage to avoid it.
    var deferred = [];
    var someMessage = "edc6f767-561e-45cb-8733-b9d1b27d7db5"; // just something that shouldn't interfere with anyone else using the window's message event
    function triggerDeferred() {
        window.postMessage(someMessage,"*");
    }
    function handleDeferred() {
        if(deferred.length != 0) {
            triggerDeferred();
            var cb = deferred.shift();
            cb();
        }
    }
    window.addEventListener("message", handleDeferred, true);

    function defer(callback) {
        deferred.push(callback);
        triggerDeferred();
    }

    function MVar() {
        var contents = undefined;
        var empty = true;
        var writeWaiters = [];
        var readWaiters = [];

        function awakenReader() {
            if(!empty && readWaiters.length != 0) {
                var cont = readWaiters.shift();
                var obj = contents;
                empty = true;
                contents = undefined;
                defer(awakenWriter);
                cont(obj);
            }
        }
        function awakenWriter() {
            if(empty && writeWaiters.length != 0) {
                var objAndCont = writeWaiters.shift();
                empty = false;
                contents = objAndCont.obj;
                defer(awakenReader);
                objAndCont.cont();
            }
        }

        this.put = function(obj, cont) {
            if(empty && writeWaiters.length == 0) {
                empty = false;
                contents = obj;
                defer(awakenReader);
                defer(cont);
            } else {
                writeWaiters.push({obj:obj,cont:cont});
            }
        }
        this.take = function(cont) {
            if(empty || readWaiters.length != 0) {
                readWaiters.push(cont);
            } else {
                var obj = contents;
                empty = true;
                contents = undefined;
                defer(awakenWriter);
                defer(function () { cont(obj) });
            }
        }
        this.terminateWaiters = function() {
            readWaiters = [];
            writeWaiters = [];
        }
    }

    function DatasyncUpload(params) {
        var self = this;

        // Stages
        self.RetrievingSignature = "get sig"
        self.ParsingSignature = "parse sig"
        self.UploadingData = "send dat"
        self.UploadingCommit = "send com"
        self.WatchingProgress = "watch prog"

        // error events
        self.AuthenticationFailed = "auth fail"
        self.ChunkUploadFailed = "send dat fail"
        self.ChunkUploadReturnedInvalidResponse = "send dat bad"
        self.ChunkUploadMalformed = "send dat mal"
        self.CommitFailed = "send com fail"
        self.CommitReturnedInvalidResponse = "send com bad"
        self.CommitMalformed = "send com mal"
        self.Forbidden = "NO"
        self.InternalError = "AUGH"
        self.OptimumBlockSizeRetrievalFailed = "get block fail"
        self.OptimumBlockSizeMalformed = "get block mal"
        self.SignatureParseFailed = "parse sig fail"
        self.SignatureRetrievalFailed = "get sig fail"
        self.SignatureRetrievalMalformed = "get sig mal"
        self.WatchFailed = "watch bad"
        self.WatchMalformed = "watch mal"

        var debuglog = params.log ? function(thing) { params.log(thing); } : function(thing) {}

        var idCtr = 0;
        var workerUrl = params.workerUrl ? params.workerUrl : "datasync-worker.js";
        if(params.forceWorkerReload) {
            workerUrl += (workerUrl.indexOf("?") == -1) ? "?" : "&";
            workerUrl += "cachebust=" + (new Date()).getTime();
        }
        var worker = new Worker(workerUrl);
        var callbacks = {};

        worker.onmessage = function(ev) {
            debuglog("Recieved " + ev.data.command + " for " + ev.data.id);
            debuglog(ev.data);
            if(!callbacks[ev.data.id]) {
                debuglog("There was no callback!");
            } else {
                callbacks[ev.data.id](ev.data);
            }
        }

        function send(msg) {
            debuglog(msg);
            worker.postMessage(msg);
        }

        function kill(job) {
            debuglog("Killing " + job);
            delete callbacks[job];
            send({id:job, command: "TERM"});
        }

        self.upload = function(datasetId, controlFile, file, params) {
            var progress = params.progress ? params.progress : function() {};
            var onError = params.onError ? params.onError : function() {};
            var onComplete = params.onComplete ? params.onComplete : function () {};
            var headers = params.headers ? $.extend({}, params.headers) : {};

            var opaque = Math.random() + " " + Math.random() + " " + Math.random() + " " + Math.random();
            controlFile = JSON.parse(JSON.stringify(controlFile)); // both clones and throws error early if this can't be JSONified somehow.
            controlFile['opaque'] = opaque;

            var stage = self.RetrievingSignature;
            var bytesRead = 0;
            var bytesTotal = file.size;
            var bytesUploaded = 0;
            var finished = false;
            var workerJob = idCtr++;
            var mvars = [];

            var datasyncBase = "/datasync";
            var versionUrl = datasyncBase + "/version.json";
            var datasetBase = datasyncBase + "/id/" + encodeURIComponent(datasetId);
            var uploadUrl = datasetBase;
            var serverSigPathBase = datasetBase + "/completed/";
            var commitUrl = datasetBase + "/commit";
            var statusUrlBase = datasetBase + "/status"
            var logUrlBase = datasetBase + "/log"
            var latestStatus = undefined;
            var latestLog = undefined;
            var terminateAfterNextLog = false;

            var _retryDelay = 1000;
            function retryDelay() {
                var r = _retryDelay;
                _retryDelay += 1000;
                if(_retryDelay > 10000) _retryDelay = 10000;
                return r;
            }
            function resetRetryDelay() {
                _retryDelay = 1000;
            }

            function endJob() {
                if(workerJob !== undefined) {
                    kill(workerJob);
                    workerJob = undefined;
                }
            }

            function cleanup() {
                finished = true;

                endJob();

                var mvar;
                while(mvar = mvars.shift()) {
                    mvar.terminateWaiters();
                }
            }

            function failure(event, data) {
                if(!finished) {
                    cleanup();
                    onError({ type : event, details : data });
                }
            }

            // Progress callback management -- this prevents calling
            // the progress callback more than every 100ms.

            var EmitProgressRate = 100;

            var EmitProgressNoTimeout = 0;     // No progress emitted in the last 100ms
            var EmitProgressTimeout = 1;       // progress was emitted in the last 100ms, but only once
            var EmitProgressTimeoutReemit = 2; // progress was emitted in the last 100ms, and another was suppressed
            var emitProgressState = EmitProgressNoTimeout;

            function currentProgress() {
                return {
                    stage: stage,
                    file_bytes_read: bytesRead,
                    file_bytes_total: bytesTotal,
                    bytes_uploaded: bytesUploaded,
                    status: latestStatus,
                    log: latestLog
                }
            }

            function reallyEmitProgress() {
                if(!finished) progress(currentProgress());
            }

            function endEmitProgressTimeout() {
                var reemit = emitProgressState == EmitProgressTimeoutReemit;
                emitProgressState = EmitProgressNoTimeout;
                if(reemit && !finished) emitProgress();
            }

            function emitProgress() {
                if(emitProgressState == EmitProgressNoTimeout) {
                    emitProgressState = EmitProgressTimeout;
                    defer(reallyEmitProgress);
                    setTimeout(endEmitProgressTimeout, EmitProgressRate);
                } else if(emitProgressState == EmitProgressTimeout) {
                    emitProgressState = EmitProgressTimeoutReemit;
                }
            }

            function parseSig2(oncomplete) {
                return function(result) {
                    if(result.command == "SIGCOMPLETE") {
                        debuglog("Parsed sig for job " + workerJob);
                        delete callbacks[workerJob];
                        oncomplete({ ok : true });
                    } else if(result.command == "SIGERROR") {
                        oncomplete({ ok: false, error : result.text })
                    } else {
                        debuglog("error: unexpected message from worker: " + result.command);
                        failure(self.InternalError, {})
                    }
                }
            }

            function parseSig1(oncomplete) {
                return function(result) {
                    if(result.command == "CHUNKACCEPTED") {
                        debuglog("Sent sig for job " + workerJob);
                        callbacks[workerJob] = parseSig2(oncomplete);
                        send({id: workerJob, command: "DONE"})
                    } else if(result.command == "SIGERROR") {
                        oncomplete({ ok: false, error : result.text })
                    } else {
                        debuglog("error: unexpected message from worker: " + result.command);
                        failure(self.InternalError, {})
                    }
                }
            }

            function parseSig0(bs, oncomplete) {
                return function(result) {
                    if(result.command == "CREATED") {
                        debuglog("Created job " + workerJob + " with " + bs.length + " bytes");
                        callbacks[workerJob] = parseSig1(oncomplete);
                        send({id: workerJob, command: "DATA", bytes: bs});
                    } else {
                        debuglog("error: unexpected message from worker: " + result.command);
                        failure(self.InternalError, {})
                    }
                }
            }

            function parseSig(targetSize, bs, oncomplete) {
                debuglog("Creating job " + workerJob + " with " + bs.length + " bytes");
                callbacks[workerJob] = parseSig0(bs, oncomplete);
                send({id: workerJob, command: "NEW", size: targetSize});
            }

            function standardXhrFailure(xhr, errorThrown, responseOnBadJSON) {
                if(xhr.status == 200) { // 200 but it's an error? -- malformed JSON
                    failure(responseOnBadJSON, {});
                } else if(xhr.status == 401 || xhr.status == 403) {
                    if(xhr.responseJSON === undefined) {
                        failure(responseOnBadJSON, {});
                    } else if(xhr.status == 401 || xhr.responseJSON.code === "authentication_required") { // ick ick ick
                        failure(self.AuthenticationFailed, xhr.responseJSON);
                    } else {
                        failure(self.Forbidden, xhr.responseJSON);
                    }
                } else {
                    return false;
                }
                return true;
            }

            function standardXhrRetry(xhr) {
                return xhr.status == 0 || xhr.status == 503; // 0 == network error, I think?
            }

            function findSig(cont) {
                var base = serverSigPathBase;

                function failed(jqXhr, _, errorThrown) {
                    if(!standardXhrFailure(jqXhr, errorThrown, self.SignatureRetrievalMalformed)) {
                        if(standardXhrRetry(jqXhr)) {
                            setTimeout(function() { findSig(cont) }, retryDelay()); // just start over
                        } else if(jqXhr.status === 404) {
                            cont(null);
                        } else {
                            failure(self.SignatureRetrievalFailed, { status : jqXhr.status })
                        }
                    }
                }

                function nextStage(resp) {
                    if(!Array.isArray(resp) || resp.length === 0) return null;
                    return resp[resp.length - 1];
                }

                $.ajax({ type: "GET", url: base, dataType: "json", headers: headers}).
                    done(function(resp) {
                        resetRetryDelay();
                        var year = nextStage(resp);
                        if(year === null) return cont(null);
                        base += year;
                        $.ajax({ type: "GET", url: base, dataType: "json", headers: headers}).
                            done(function(resp) {
                                resetRetryDelay();
                                var month = nextStage(resp);
                                if(month === null) return cont(null);
                                base += month;
                                $.ajax({ type: "GET", url: base, dataType: "json", headers: headers}).
                                    done(function(resp, status) {
                                        resetRetryDelay();
                                        var day = nextStage(resp);
                                        if(day === null) return cont(null);
                                        base += day + "signatures/";
                                        $.ajax({ type: "GET", url: base, dataType: "json", headers: headers}).
                                            done(function(resp) {
                                                resetRetryDelay();
                                                var file = nextStage(resp);
                                                if(file === null) return cont(null);
                                                var result = base + resp[resp.length - 1];
                                                cont(result);
                                            });
                                    }).
                                    fail(failed);
                            }).
                            fail(failed);
                    }).
                    fail(failed);
            }

            function findChunkSize(cont) {
                $.ajax({ type: "GET", url: versionUrl, dataType: "json", headers: headers}).
                    done(function(resp) {
                        resetRetryDelay();
                        // TODO: validate resp, including blockSize bounds
                        var blockSize = resp['max-block-size'];
                        cont(blockSize);
                    }).
                    fail(function(jqXhr, _, errorThrown) {
                        if(!standardXhrFailure(jqXhr, errorThrown, self.OptimumBlockSizeMalformed)) {
                            if(standardXhrRetry(jqXhr)) {
                                setTimeout(function() { findChunkSize(cont); }, retryDelay());
                            } else {
                                failure(self.OptimumBlockSizeRetrievalFailed, { status : jqXhr.status })
                            }
                        }
                    });
            }

            // This uploading process is rather involved.  If we're
            // doing a patch, then there are three (logical) threads
            // involved: the file reader, the patch computer, and the
            // chunk uploader.  If there is no signature file, then
            // there is no patch and hence only the file reader and
            // the file uploader.
            //
            // The different threads communicate via MVars.  The file
            // reader writes into an MVar objects of the forms
            //    { done : false, bytes: Uint8Array }
            //    { done : true }
            // The chunk uploader reads from an MVar (the same MVar if
            // no patch is happening) objects of those same forms.
            //
            // This leaves the patch computer "thread", which is
            // simultaneously the only real thread (because the bulk
            // of it is in a web worker) and a pair of helper
            // semithreads (in shovelIntoWorker and extractFromWorker)
            // that mediate the access to that worker.  One semithread
            // is responsible for posting data to the worker; it
            // receives data from the file reader and waits for the
            // chunk to be acknowledged by the worker.  The other
            // semithread receives messages from the worker,
            // interprets them, and writes into MVars as appropriate.
            // The worker's "chunk received" acknowledgements are
            // received by this semithread; it communicates the ack
            // back to the source semithread vi a unit MVar.

            function readComplete(reader, targetMVar, chunkSize) {
                return function(e) {
                    if(!finished) {
                        emitProgress();
                        targetMVar.put({done: false, bytes : new Uint8Array(e.target.result)}, function() {
                            readFileInto(reader, targetMVar, chunkSize);
                        });
                    }
                }
            }

            function readFileInto(reader, targetMVar, chunkSize) {
                if(bytesRead >= bytesTotal) {
                    targetMVar.put({done:true}, function() {});
                } else {
                    var offset = bytesRead;
                    debuglog("Reading from " + offset);
                    bytesRead = Math.min(bytesTotal, offset + chunkSize);
                    reader.readAsArrayBuffer(file.slice(offset, bytesRead));
                }
            }

            function sendDataFrom(sourceMVar, cont, chunksSent) {
                if(chunksSent === undefined) chunksSent = [];
                sourceMVar.take(function(data) {
                    if(data.done) {
                        debuglog("finished uploading chunks, proceeding to the commit");
                        cont(chunksSent);
                    } else {
                        debuglog("sending a chunk");
                        debuglog(data);
                        var chunkLen = data.bytes.length;
                        var sendLoop = function() {
                            $.ajax({
                                dataType: 'json',
                                type: 'POST',
                                url: uploadUrl,
                                data: data.bytes,
                                processData: false,
                                contentType: 'application/octet-stream',
                                headers: headers
                            }).done(function(response) {
                                debuglog("sent a chunk")
                                resetRetryDelay();
                                if(!finished) {
                                    emitProgress();
                                    if(response.blobId === undefined) {
                                        debuglog("...but the response didn't contain a blob ID?")
                                        failure(self.ChunkUploadReturnedInvalidResponse, { response: response })
                                    } else {
                                        chunksSent.push(response.blobId);
                                        bytesUploaded += chunkLen;
                                        defer(function() { sendDataFrom(sourceMVar, cont, chunksSent) });
                                    }
                                } else {
                                    debuglog("...but we're finished");
                                }
                            }).fail(function(jqXhr, _, errorThrown) {
                                if(!standardXhrFailure(jqXhr, errorThrown, self.ChunkUploadMalformed)) {
                                    if(standardXhrRetry(jqXhr)) {
                                        setTimeout(sendLoop, retryDelay());
                                    } else {
                                        failure(self.ChunkUploadFailed, { status : jqXhr.status })
                                    }
                                }
                            });
                        }
                        sendLoop();
                    }
                });
            }

            function shovelIntoWorker(sourceMVar, readyMVar) {
                debuglog("waiting for next chunk from file");
                sourceMVar.take(function(data) {
                    if(data.done) {
                        debuglog("file is finished, telling the worker so and terminating shovelIntoWorker")
                        send({id: workerJob, command: "DONE"});
                    } else {
                        debuglog("received block of file data; waiting for worker to be ready for a chunk");
                        readyMVar.put({}, function() {
                            debuglog("worker is ready; sending chunk to worker");
                            send({id: workerJob, command: "DATA", bytes: data.bytes});
                            shovelIntoWorker(sourceMVar, readyMVar);
                        });
                    }
                })
            }

            function extractFromWorker(targetMVar, readyMVar) {
                callbacks[workerJob] = function(result) {
                    debuglog("received " + result.command + " from worker");
                    if(result.command == "CHUNKACCEPTED") {
                        debuglog("telling shovelIntoWorker that its the worker is ready for another chunk");
                        readyMVar.take(function() { /* don't need to do anything else */ })
                    } else if(result.command == "CHUNK") {
                        debuglog("received chunk of patch data; sending data to sender");
                        targetMVar.put({ done: false, bytes : result.bytes },
                                       function() {
                                           debuglog("sent data");
                                           send({ id : workerJob, command : "CHUNKACCEPTED" });
                                       });
                    } else if(result.command == "COMPLETE") {
                        endJob();
                        debuglog("sending `done' to sender");
                        targetMVar.put({ done: true }, function() {
                            debuglog("sent `done'");
                        });
                    } else {
                        debuglog("error: unexpected message from worker: " + result.command);
                        failure(self.InternalError, {})
                    }
                }
            }

            function fetchLoop(url, withResponse) {
                if(!finished) {
                    $.ajax({ type: "GET",
                             url: url,
                             dataType: "json",
                             headers: headers }).
                        done(function(response) {
                            resetRetryDelay();
                            if(!finished) {
                                emitProgress();
                                var nextStep = withResponse(response);
                                if(nextStep === undefined) {
                                    setTimeout(function () { fetchLoop(url, withResponse) }, 6000);
                                } else {
                                    nextStep();
                                }
                            }
                        }).fail(function(jqXhr, _, errorThrown) {
                            if(!standardXhrFailure(jqXhr, errorThrown, self.WatchMalformed)) {
                                if(standardXhrRetry(jqXhr)) {
                                    setTimeout(function() { fetchLoop(url, withResponse) }, retryDelay());
                                } else {
                                    failure(self.WatchFailed, { status: jqXhr.status });
                                }
                            }
                        })
                }
            }

            function fetchStatusLoop(jobId) {
                fetchLoop(statusUrlBase + "/" + jobId + ".json", function(status) {
                    latestStatus = status;
                    if(status.type == 'success' || status.type == 'failure') {
                        terminateAfterNextLog = true;
                        return emitProgress;
                    } else {
                        return undefined;
                    }
                });
            }

            function fetchLogLoop(jobId) {
                fetchLoop(logUrlBase + "/" + jobId + ".json", function(log) {
                    latestLog = log;
                    if(terminateAfterNextLog) {
                        cleanup();
                        return function() {
                            onComplete(currentProgress());
                        }
                    } else {
                        return undefined;
                    }
                });
            }

            function commit(filename, signatureUrl) {
                return function(chunks) {
                    debuglog(chunks);
                    var commitSpec = {
                        filename: filename,
                        chunks: chunks,
                        relativeTo: signatureUrl,
                        expectedSize: bytesUploaded,
                        control: controlFile
                    };
                    stage = self.UploadingCommit;
                    emitProgress();
                    $.ajax({ type: "POST",
                             url: commitUrl,
                             data: JSON.stringify(commitSpec),
                             dataType: "json",
                             headers: headers }).
                        done(function(response) {
                            resetRetryDelay();
                            var jobId = response.jobId;
                            if(jobId === undefined) {
                                failure(self.ChunkUploadReturnedInvalidResponse, { response: response })
                            } else {
                                stage = self.WatchingProgress;
                                fetchStatusLoop(jobId);
                                fetchLogLoop(jobId);
                            }
                        }).
                        fail(function(jqXhr, _, errorThrown) {
                            if(!standardXhrFailure(jqXhr, errorThrown, self.CommitMalformed)) {
                                // TODO: network errors
                                // When retry is implemented, check the dataset's log for the opaque value
                                failure(self.CommitFailed, { status : jqXhr.status })
                            }
                        })
                }
            }

            function straightUpload(chunkSize) {
                // There is no signature on the server *sigh*
                // so just read the file in chunks and upload it straight
                stage = self.UploadingData;
                emitProgress();

                var reader = new FileReader();
                var currentBlock = new MVar();
                mvars.push(currentBlock);

                reader.onload = readComplete(reader, currentBlock, chunkSize);
                readFileInto(reader, currentBlock, chunkSize);
                sendDataFrom(currentBlock, commit(file.name, null));
            }

            function doPatchedUpload() {
                stage = self.UploadingData;
                emitProgress();

                var reader = new FileReader();
                var diskBlock = new MVar();
                var netBlock = new MVar();
                var ready = new MVar(); // this is EMPTY when the worker is willing to receive a new chunk
                mvars.push(diskBlock, netBlock, ready);

                var chunkSize = 81920;
                reader.onload = readComplete(reader, diskBlock, chunkSize);
                readFileInto(reader, diskBlock, chunkSize);
                shovelIntoWorker(diskBlock, ready);
                extractFromWorker(netBlock, ready);
                sendDataFrom(netBlock, commit(file.name + ".sdiff", signatureUrl));
            }

            function patchedUpload(sigurl, chunkSize) {
                debuglog("Fetching signature from " + sigurl + " ...");
                var xhr = new XMLHttpRequest();
                xhr.onreadystatechange = function() {
                    if(this.readyState == 4) {
                        if(this.status != 200) {
                            var errorThrown = undefined;
                            try {
                                // TODO: I don't think TextDecoder is supported by IE
                                this.responseJSON = $.parseJSON(new TextDecoder("UTF-8").decode(new Uint8Array(this.response)))
                                errorThrown = null;
                            } catch (e) {
                                errorThrown = e;
                            }
                            if(!standardXhrFailure(this, errorThrown, null /* this can't be malformed */)) {
                                if(this.status == 404) {
                                    // signature expired right before we
                                    // could get to it so we'll just do a
                                    // normal upload
                                    straightUpload(chunkSize);
                                } else {
                                    failure(self.SignatureRetrievalFailed, { status: this.status });
                                }
                            }
                        } else {
                            debuglog("Parsing signature...");
                            stage = self.ParsingSignature
                            emitProgress();
                            parseSig(chunkSize, new Uint8Array(this.response), function(result) {
                                if(result.ok) {
                                    doPatchedUpload()
                                } else {
                                    failure(self.SignatureParseFailed, result.error)
                                }
                            });
                        }
                    }
                }
                xhr.open('GET', sigurl, true);
                xhr.responseType = 'arraybuffer';
                for(var h in headers) {
                    if(headers.hasOwnProperty(h)) xhr.setRequestHeader(h, headers[h]);
                }
                xhr.send();
            }

            function process(signatureUrl, chunkSize) {
                if(signatureUrl === null) straightUpload(chunkSize);
                else patchedUpload(signatureUrl, chunkSize);
            }

            var signatureUrl = undefined;
            var chunkSize = undefined;
            function receiveInit(from) {
                if(signatureUrl !== undefined && chunkSize !== undefined) {
                    process(signatureUrl, chunkSize);
                }
            }
            findSig(function(sig) { signatureUrl = sig; receiveInit("findSig"); });
            findChunkSize(function(cs) { chunkSize = cs; receiveInit("chunkSize"); });
            defer(reallyEmitProgress);
        }
    }
    return DatasyncUpload;
}(jQuery));
