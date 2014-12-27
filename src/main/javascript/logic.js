function debuglog(thing) {
    // console.log(thing);
}

// encapsulation, what's that?
// note: this is NOT pure RPC!  When generating the patch,
// the produced data will be sent back here asynchronously.
function SSync() {
    var self = this;

    self.idCtr = 0;
    self.worker = new Worker("all.js");
    self.callbacks = {};

    self.worker.onmessage = function(ev) {
        debuglog("Recieved " + ev.data.command + " for " + ev.data.id);
        debuglog(ev.data);
        if(!self.callbacks[ev.data.id]) {
            debuglog("There was no callback!");
        } else {
            self.callbacks[ev.data.id](ev.data);
        }
    }

    self.send = function(msg) {
        debuglog(msg);
        self.worker.postMessage(msg);
    }

    self.kill = function(job) {
        debuglog("Killing " + job);
        delete self.callbacks[job];
        self.send({id:job, command: "TERM"});
    }
}

var ssync = new SSync();

function AsyncBox(name, size) {
    if(!size) size = 1;
    var self = this;
    self.waitingForEmpty = [];
    self.waitingForFull = [];
    self.values = [];
    self.closed = false;

    var putIntl = function(v, continuation, blocked) {
        if(self.closed) {
            continuation({ok : false});
        } else if(self.values.length >= size) {
            debuglog("PUT: blocking in " + name);
            self.waitingForEmpty.push(function() {
                debuglog("PUT: unblocked in " + name);
                putIntl(v, continuation, true);
            });
        } else {
            self.values.push(v);
            if(self.waitingForFull.length != 0) {
                debuglog("PUT: unblocking a thread in " + name);
                var waiterCont = self.waitingForFull.shift();
                waiterCont();
            }
            debuglog("PUT: successfully put into " + name);
            continuation({ ok : true, blocked : blocked });
        }
    }

    self.put = function(v, continuation) {
        putIntl(v, continuation, false);
    }

    var takeIntl = function(continuation, blocked) {
        if(self.closed) {
            continuation({ ok : false });
        } else if(self.values.length != 0) {
            var result = self.values.shift();
            if(self.waitingForEmpty.length != 0) {
                debuglog("take: unblocking a thread in " + name);
                var waiterCont = self.waitingForEmpty.shift();
                waiterCont();
            }
            debuglog("take: successfully took from " + name);
            continuation({ ok : true, blocked : blocked, result : result });
        } else {
            debuglog("take: blocking in " + name);
            self.waitingForFull.push(function() {
                debuglog("take: unblocked in " + name);
                takeIntl(continuation, true);
            });
        }
    }

    self.take = function(continuation) {
        takeIntl(continuation, false);
    }

    self.close = function() {
        self.closed = true;
        while(self.waitingForEmpty.length != 0) {
            var waiterCont = self.waitingForEmpty.shift();
            waiterCont();
        }
        while(self.waitingForFull.length != 0) {
            var waiterCont = self.waitingForFull.shift();
            waiterCont();
        }
    }
}

function parseSig2(job, oncomplete) {
    return function(result) {
        if(result.command == "SIGCOMPLETE") {
            debuglog("Parsed sig for job " + job);
            ssync.callbacks[job] = undefined;
            oncomplete({ok : true, continuation: job});
        } else {
            ssync.kill(job)
            oncomplete({ok : false, error: result})
        }
    }
}

function parseSig1(job, oncomplete) {
    return function(result) {
        if(result.command == "CHUNKACCEPTED") {
            debuglog("Sent sig for job " + job);
            ssync.callbacks[job] = parseSig2(job, oncomplete);
            ssync.send({id: job, command: "DONE"})
        } else {
            ssync.kill(job)
            oncomplete({ok : false, error: result})
        }
    }
}

function parseSig0(job, bs, oncomplete) {
    return function(result) {
        if(result.command == "CREATED") {
            debuglog("Created job " + job + " with " + bs.length + " bytes");
            ssync.callbacks[job] = parseSig1(job, oncomplete);
            ssync.send({id: job, command: "DATA", bytes: bs});
        } else {
            oncomplete({ok : false, error: result})
        }
    }
}

function parseSig(targetSize, bs, oncomplete) {
    var job = ssync.idCtr++;
    debuglog("Creating job " + job + " with " + bs.length + " bytes");
    ssync.callbacks[job] = parseSig0(job, bs, oncomplete);
    ssync.send({id: job, command: "NEW", size: targetSize});
}

function fakeSig(targetSize, oncomplete) {
    var job = ssync.idCtr++;
    debuglog("Creating job " + job);
    ssync.callbacks[job] = function(result) {
        if(result.command == "CREATED") {
            oncomplete({ok: true, continuation : job})
        } else {
            oncomplete({ok: false, error: result});
        }
    };
    ssync.send({id: job, command: "NEWFAKESIG", size: targetSize});
}

function msg(s) {
    $("#message").text(s);
}

function ds_status(s) {
    $("#status").text(s);
}

function ds_log(s) {
    $("#log").text(s); // append(document.createTextNode(s));
}

function readFile_progress(state) {
    state.progress(state.currentOffset, state.endOffset, state.uploadedSize);
}

function readFile_dataFromFile(state) {
    return function(e) {
        state.fromReader.put({done:false, bytes: e.target.result}, function(res) {
            debuglog("putting bytes in fromReader blocked: " + res.blocked);
            if(res.ok) {
                readFile_readNextChunk(state);
            } else {
                // process was terminated; just stop doing things
            }
        });
    }
}

function readFile_readNextChunk(state) {
    if(state.currentOffset >= state.endOffset) {
        state.fromReader.put({done:true}, function(res) {
            debuglog("putting done in fromReader blocked: " + res.blocked);
        });
        readFile_progress(state);
    } else {
        var offset = state.currentOffset;
        state.currentOffset = Math.min(state.endOffset, offset + state.chunkSize);
        state.reader.readAsArrayBuffer(state.file.slice(offset, state.currentOffset));
        readFile_progress(state);
    }
}

function readFile_awaitChunkFromFile(state) {
    return function(res) {
        debuglog("take from fromReader blocked: " + res.blocked);
        if(res.ok) {
            debuglog("***********************");
            debuglog(res);
            if(res.result.done) {
                ssync.send({id: state.job, command: "DONE"});
            } else {
                ssync.send({id: state.job, command: "DATA", bytes: new Uint8Array(res.result.bytes)});
            }
            // do NOT take again; the CHUNKACCEPTED response will do it.
        } else {
            ssync.kill(state.job);
        }
    }
}

function readFile_messageFromWorker(state) {
    return function(result) {
        debuglog("received " + result.command);
        if(result.command == "CHUNKACCEPTED") {
            state.fromReader.take(readFile_awaitChunkFromFile(state));
        } else if(result.command == "CHUNK") {
            state.fromWorker.put({ done: false, chunk : result.bytes }, function(res) {
                debuglog("putting chunk in fromWorker blocked: " + res.blocked);
                if(res.ok) {
                    // ok good
                }
            });
        } else if(result.command == "COMPLETE") {
            state.fromWorker.put({ done: true }, function(res) {
                debuglog("putting done in fromWorker blocked: " + res.blocked);
                ssync.kill(state.job);
            });
        } else {
            debuglog("error: unexpected message from worker: " + result.command);
            readFile_close(state);
        }
    }
}

function readFile_dataFromWorker(state) {
    return function(res) {
        debuglog("take from fromWorker blocked: " + res.blocked);
        if(res.ok) {
            if(res.result.done) {
                state.done(state.uploadedChunks, state.uploadedSize);
            } else {
                // TODO: this needs to retry on errors
                var chunklen = res.result.chunk.length;
                $.ajax({
                    dataType: 'json',
                    type: 'POST',
                    url: state.uploadUrl,
                    data : res.result.chunk,
                    processData: false,
                    contentType: 'application/octet-stream',
                    headers : state.authHeaders
                }).done(function(response) {
                    // TODO: this send needs to happen in all error cases too (after any retrying is done!)
                    ssync.send({ id : state.job, command : "CHUNKACCEPTED" });
                    state.uploadedChunks.push(response.blobId);
                    state.uploadedSize += chunklen;
                    readFile_progress(state);
                    state.fromWorker.take(readFile_dataFromWorker(state));
                })
            }
        } else {
            ssync.kill(state.job);
        }
    }
}

function readFile_close(state) {
    ssync.kill(state.job);
    state.fromReader.close();
    state.fromWorker.close();
}

function readFile(job, file, progress, chunk, done, uploadUrl, authHeaders) {
    // ok, here's where the control flow gets a little hairy.
    // We have three asynchronous processes -- files reading,
    // chunks processing and data uploading -- and we want to
    // manage all of them.  But we also want to exploit the
    // asynchronicity!  So we'll do anything only if we're not
    // already doing such a thing.

    var state = {
        // constant data
        job: job,
        reader: new FileReader(),
        file: file,
        chunkSize: 10240, // size of each read from the file
        uploadUrl: uploadUrl,
        authHeaders: authHeaders,

        // callbacks
        progress: progress,
        chunk: chunk,
        done: done,

        // variableData
        currentOffset: 0,
        endOffset: file.size,
        uploadedChunks: [],
        uploadedSize: 0,

        // async status tracking
        fromReader : new AsyncBox("reader"),
        fromWorker : new AsyncBox("worker")
    };
    ssync.callbacks[job] = readFile_messageFromWorker(state);
    state.reader.onload = readFile_dataFromFile(state);
    state.fromReader.take(readFile_awaitChunkFromFile(state));
    state.fromWorker.take(readFile_dataFromWorker(state));
    readFile_readNextChunk(state);
}

function watchUrl(url, authHeaders, withResult, finished) {
    $.ajax({ type: "GET", url: url, dataType: "text", headers: authHeaders }).done(function(resp) {
        withResult(resp);
        if(!finished(resp)) {
            setTimeout(function() { watchUrl(url, authHeaders, withResult, finished); }, 6000);
        }
    });
}

function watchProgress(statusUrl, logUrl, authHeaders) {
    var statusDone = false;
    var logDone = false;
    watchUrl(statusUrl, authHeaders, ds_status, function(s) {
        statusDone = (s.indexOf("SUCCESS:") === 0) || (s.indexOf("FAILURE:") === 0);
        return statusDone;
    });
    watchUrl(logUrl, authHeaders, ds_log, function(s) {
        var result = logDone;
        logDone = statusDone;
        return result;
    });
}

function findSig(base, authHeaders, cont) {
    $.ajax({ type: "GET", url: base, dataType: "json", headers: authHeaders}).done(function(resp) {
        if(resp.length == 0) {
            cont(null);
        } else {
            var year = resp[resp.length - 1];
            base += year;
            $.ajax({ type: "GET", url: base, dataType: "json", headers: authHeaders}).done(function(resp) {
                var month = resp[resp.length - 1];
                base += month;
                $.ajax({ type: "GET", url: base, dataType: "json", headers: authHeaders}).done(function(resp) {
                    var day = resp[resp.length - 1];
                    base += day + "signatures/";
                    $.ajax({ type: "GET", url: base, dataType: "json", headers: authHeaders}).done(function(resp) {
                        var result = base + resp[resp.length - 1];
                        cont(result);
                    });
                });
            });
        }
    });
}

function go() {
    // $("#go").prop("disabled", true);
    $("#log").empty();
    $("#status").empty();
    $("#message").empty();
    var file = $("#input")[0].files[0];
    var ff = $("#fourfour").val();
    var datasyncBase = "/datasync"
    var versionUrl = datasyncBase + "/version.json"
    var datasetBase = datasyncBase + "/id/" + ff
    var serverSigPathBase = "/datasync/id/" + ff + "/completed/";
    var uploadUrl = datasetBase
    var commitUrl = datasetBase + "/commit"
    var statusUrlBase = datasetBase + "/status"
    var logUrlBase = datasetBase + "/log"
    var appToken = $("#apptoken").val();
    var authHeaders = {};
    if($.cookie('socrata-csrf-token')) {
        authHeaders["x-csrf-token"] = encodeURIComponent($.cookie('socrata-csrf-token'));
    }
    findSig(serverSigPathBase, authHeaders, function(sigurl) {
        msg("Retrieving target file size...");
        $.getJSON(versionUrl, function(j) {
            var blockSize = j['max-block-size'];

            var generatePatch = function(job) {
                msg("Yay parsed! -- next up: doing the file");
                var progress = function(read, total, sent) {
                    msg("read " + Math.round(100000*read/total)/1000 + "%; sent " + sent + " bytes");
                }
                var chunk = function(c) {
                    out(c);
                }
                var finished = function(chunks, size) {
                    msg("Uploaded CSV (" + file.size + " bytes; required " + size + " bytes to be uploaded)");
                    var commitSpec = { filename : file.name + ".sdiff", chunks : chunks, relativeTo: sigurl, expectedSize : size, control : { csv : { action: "replace" } } };
                    // TODO errors
                    $.ajax({ type: "POST", url: commitUrl, data: JSON.stringify(commitSpec), dataType: "json", headers: authHeaders }).done(function(resp) {
                        watchProgress(statusUrlBase + "/" + resp.jobId + ".txt", logUrlBase + "/" + resp.jobId + ".txt", authHeaders);
                    })
                }
                if(appToken) authHeaders['X-App-Token'] = appToken;
                readFile(job, file, progress, chunk, finished, uploadUrl, authHeaders);
            }

            if(sigurl == null) {
                msg("No signature available; using empty signature")
                fakeSig(blockSize, function(result) {
                    if(result.ok) {
                        generatePatch(result.continuation);
                    } else {
                        msg(":((((( -- " + JSON.stringify(result.error));
                    }
                });
            } else {
                msg("Fetching signature from " + sigurl + " ...");
                var xhr = new XMLHttpRequest();
                xhr.onreadystatechange = function() {
                    if(this.readyState == 4 && this.status == 200) {
                        msg("Parsing signature...");
                        // wheeeeee!
                        var reader = new FileReader();
                        reader.addEventListener("loadend", function() {
                            parseSig(blockSize, new Uint8Array(reader.result), function(result) {
                                if(result.ok) {
                                    generatePatch(result.continuation)
                                } else {
                                    msg(":((((( -- " + JSON.stringify(result.error));
                                }
                            });
                        });
                        reader.readAsArrayBuffer(this.response);
                    }
                }
                xhr.open('GET', sigurl, true);
                xhr.responseType = 'blob';
                if(appToken) xhr.setRequestHeader('X-App-Token', appToken);
                xhr.send();
            }
        }).fail(function() {
            msg("error reading target size")
        })
    });
}
