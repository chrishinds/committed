async = require 'async'

_started = false
_queues = null
_db = null
_registry = {}

_softwareVersion = null
_queueCountersCollection = null
_transactionsCollection = null


exports.start = (db, softwareVersion, done) ->
    if _started
        return done( new Error("committed has already been started") )
    _softwareVersion = softwareVersion
    _db = db

    tasks = []

    tasks.push (done) ->
        #create the transactionsCollection
        _db.createCollection 'transactions', {w:1, journal: true}, (err, collection) ->
            if err?
                return done(err, null)
            #NEED TO PUT SOME INDEXES ON TRANSACTION EG ON STATUS ***************************************************************************
            _transactionsCollection = collection
            return done(null, null)

    tasks.push (done) ->
        #create the transactionsCollection
        _db.createCollection 'queueCounters', {w:1, journal: true}, (err, collection) ->
            if err?
                return done(err, null)
            _queueCountersCollection = collection
            return done(null, null)

    tasks.push (done) ->
        #find any tasks in the transaction queues which are stuck at
        #'Processing' status and roll them back rollback should then leave the
        #transaction at a 'Failed' status, which is at least safe we'll sort
        #by queue position, different queues could be interspliced, but queues
        #should be semantically independent so that isnt problematic, what
        #important is that the sequence of each transaction is respected
        #within the queue take the transactions in ascending sequence order.
        #Any transactions resulting from a committed.immediately will have a
        #position of -1 so will be attempted first
        _transactionsCollection.find({status: 'Processing'}, {snapshot: true}).sort 'position', 1, (err, transactions) ->
            if err? then return done(err, null)
            #do the rollback quietly, we don't want an error to interrupt the async.series
            async.eachSeries transactions, _quietly(rollback), done


    tasks.push (done) ->
        #restart the queues, actually it's easier just to execute all
        #transactions in series. it would be possible to form new queues and
        #do this asynchronously. if startup is slow then look at upgrading
        #this. again, committed.immediately transactions with position = -1
        #will be done first
        _transactionsCollection.find({status: 'Queued'}, {snapshot: true}).sort 'position', 1, (err, transactions) ->
            #similar to above, do the commit quietly so that errors don't abort the async.series.
            async.eachSeries transactions, _quietly(commit), done


    #run the start-up tasks
    async.series tasks, (err, results) ->
        if err? then return done(err)
        #re-create the _queue object, initially this should just contain the
        #GlobalLock queue, others will be created as needed. We create the
        #GlobalLock queue here at the end of start because stop will reset the
        #drain (for good reason) and so start needs to put it back.
        _queues =
            GlobalLock: async.queue(commit, 1)
        _queues.GlobalLock.drain = () ->
            #if our global lock queue is now empty, make sure to restart the other queues
            _started = true
        #now start the queues for the first time
        _started = true
        return done(null)


#to end it all: we should stop accepting new transactions and drain off all the queues
#this should include draining the GlobalLock queue, and we should be careful to respect how _started is used by GlobalLock
exports.stop = (done) ->
    if not _started
        return done( new Error("committed is not currently started") )
    #stop accepting new transactions
    _started = false
    #drain every queue we have available (even the GlobalLock)
    _drainQueues ( name for name of _queues ), done


_drainQueues = (queues, done) ->
    #create a task to drain each queue
    tasks = []
    for name in queues
        do (name) ->
            tasks.push (done) ->
                if _queues[name].length() is 0
                    #if queue is empty then nothing more to do
                    return done(null, null)
                else
                    #add a drain to the queue to callback after the last task is completed
                    _queues[name].drain = () ->
                        done(null, null)
    #run the drain tasks and wait for the queues to empty
    async.parallel tasks, (err, results) ->
        #unset the drain of every queue
        _queues[name].drain = undefined for name in queues
        done(err)


#can throw Error
exports.register = (name, fnOrObj) ->
    parent = _registry
    #find the parent within the registry for the given (possibly dot-separated) name
    ancestors = name.split('.')
    key = ancestors.pop() #modifies ancestors, obv
    for ancestor in ancestors
        parent = parent[ancestor]
        if not parent?
            throw new Error("during register: path #{name} doesn't exist in registry: #{JSON.stringify(_registry)}")
    if parent[key]?
        throw new Error("during register: path #{name} already exists in registry: #{JSON.stringify(_registry)}")
    #set the registry to record the given function or Object
    parent[key] = fnOrObj


_queuePosition = (queueName, done) ->
    #findAndModify will return the old object, not the updated one
    _queueCountersCollection.findAndModify(
        {queue: queueName},
        {},
        {$inc:{nextPosition: 1}},
        {upsert: true, w: 1, journal: true},
        (err, doc) ->
            if err? then return done(err, null)
            done(null, doc.nextPosition ? 0)
    )


_enqueueOrCreateAndEnqueue = (queueName, transaction, done) ->
    if not _queues[queueName]?
        _queues[queueName] = async.queue(commit, 1) #the commit fn will be the queue worker; only one worker per queue for sequentiality
        #note: we could add a drain to this queue to delete on completion. this would minimise memory at the expense of speed.
    _queues[queueName].push transaction, done


exports.sequentially = (transaction, done) ->
    #check we've got a valid queuename in the transaction.
    if transaction.queue is "GlobalLock" then return done( new Error("Can't queue a transaction for GlobalLock using the committed.sequentially function"), null)
    if not transaction.queue? then return done( new Error("must have a transaction.queue parameter to use committed.sequentially"), null)
    #if we have got here then we intend at least to save the transaction, so set some essential values
    transaction.status = if _started then "Queued" else "Failed"
    #this includes setting the queue position
    _queuePosition transaction.queue, (err, position) ->
        transaction.position = position
        return _transactionsCollection.insert transaction, (err, docs) ->
            if err? then return done(err, null)
            #transaction has been written to the db, we're ready to enqueue, or bail if we've failed
            if _started
                return _enqueueOrCreateAndEnqueue transaction.queue, transaction, done
            else
                return done(null, 'Failed')


exports.immediately = (transaction, done) ->
    #no queue necessary, commit straight away, useful for inserts, but where you need a transaction as an audit record.
    # but as above, respect _started, and fail otherwise.
    if transaction.queue?
        return done( new Error("Can't call committed.immediately on a transaction which has a queue defined for it"))
    transaction.status = if _started then "Queued" else "Failed"
    #if we set the position to -1 then during start up if we want to process an immediate task it will get done first.
    transaction.position = -1
    _transactionsCollection.insert transaction, (err, docs) ->
        if err? then return done(err, null)
        #transaction has been written to the db, it can be executed, immediately
        if _started
            return commit transaction, done
        else
            return done(null, 'Failed')


exports.withGlobalLock = (transaction) ->
    #useful if you want to safely update fixtures, or do aggregate calculations.
    # magic "GlobalLock" queue, with special drain function, that's set up during start
    if transaction.queue isnt "GlobalLock" then return done( new Error("Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"), null)

    #set essential values so the transaction can be saved
    transaction.status = "Queued"
    #this includes setting the queue position
    _queuePosition transaction.queue, (err, position) ->
        transaction.position = position
        return _transactionsCollection.insert transaction, (err, docs) ->
            if err? then return done(err, null)
            #now the transaction's inserted, stop accepting new (non-global) transactions
            _started = false
            #drain all non-GlobalLock queues
            _drainQueues ( name for name of _queues when name isnt 'GlobalLock' ), (err) ->
                if err? then return done(err, null)
                #we will rely on the special drain set up on GlobalLock made
                #during committed.start, to restore business as usual
                _queues['GlobalLock'].push transaction, done


applyToRegistered = (name, fnArgs) ->
    found = _registry
    for key in name.split('.')
        found = found[key]
        if not found?
            return done( new Error("during applyToRegistered: path #{name} doesn't exist in registry: #{JSON.stringify(_registry)}") )
    # db is always the first argument for any registered function
    found.apply(@, fnArgs)


#done(err, result)
execute = (instructions, data, done) ->
    result = true
    iterator = (instruction, iteratorDone) ->
        #assemble the arguments for the call of one of our registered functions
        fnArgs = [_db, data].concat(instruction.arguments)
        #add the callback
        fnArgs.push (err, iteratorResult) ->
            if iteratorResult is false then result = false
            iteratorDone(err)
        #call the function itself
        applyToRegistered instruction.name, fnArgs
    #run instructions in parallel
    async.each instructions, iterator, (err) ->
        done(err, result)


_quietly = (fn) ->
    return (args..., done) ->
        quietDone = (err, result) ->
            if err?
                #this needs to go out to some kind of error notification...
                console.log "error suppressed, probably during committed.start: #{JSON.stringify err}"
            done(null, result)
        return fn(args..., quietDone)

_pushTransactionError = (transaction, error, done) ->
    transaction.errors.push error
    _transactionsCollection.update {_id: transaction._id}, {$push: {errors: error.toString()}}, {w:1, journal: true}, (err, updated) ->
        #log these errors but don't pass them up
        if err? then console.log "error saving error to transaction: #{err}"
        done()

_updateTransactionStatus = (transaction, fromStatus, toStatus, done) ->
    _transactionsCollection.update {_id: transaction._id, status: fromStatus}, {$set: {status: toStatus, softwareVersion: _softwareVersion}}, {w:1, journal: true}, (err, updated) ->
        switch
            when err?
                _pushTransactionError transaction, err, () ->
                    return done(err)
            when updated isnt 1
                #then the transaction in the database doesn't match the one we have in memory; we're in bad shape so bail out
                updateError = new Error('exactly one transaction must be updated when moving to Processing status')
                _pushTransactionError transaction, updateError, () ->
                    return done(updateError)
            else
                return done(null)


#if rollback succeeds then set transaction.status = Failed; if rollback fails set to failStatus
rollback = (transaction, failStatus, done) ->
    if transaction.status isnt 'Processing' then return done( new Error("can't rollback a transaction that isn't at 'Processing' status"), null)
    execute transaction.rollback, transaction.data, (rollbackErr, result) ->
        failed = err? or result is false
        switch
            when failed and transaction.errors.length > 0
                newStatus = 'CatastropheCommitErrorRollbackError'
            when failed and transaction.errors.length is 0
                newStatus = 'CatastropheCommitFailedRollbackError'
            when not failed and transaction.errors.length > 0
                newStatus = 'FailedCommitErrorRollbackOk'
            when not failed and transaction.errors.length is 0
                newStatus = 'Failed'

        #we used the length of transaction.errors above to tell whether
        #there were errors during a commit, now that's done we can add any
        #errors implied by the rollback
        _updateTransactionStatus transaction, 'Processing', newStatus, (statusErr) ->
            if statusErr?
                done(statusErr, newStatus)
            else if rollbackErr?
                _pushTransactionError transaction, rollbackErr, () ->
                    done(err, newStatus)
            else
                done(null, newStatus)


commit = (transaction, done) ->
    console.log _queues['contact'].length()
    #is this transaction at queued status?
    if transaction.status isnt 'Queued' then return done( new Error("can't begin work on a transaction which isn't at 'Queued' status (#{transaction.status}"), null )
    transaction.status = 'Processing'
    _updateTransactionStatus transaction, 'Queued', 'Processing', (err) ->
        if err? then return done(err, null)
        #transaction is now at 'Processing' status, we can now execute its instructions.
        execute transaction.instructions, transaction.data, (err, result) ->
            if err?
                _pushTransactionError transaction, err, () ->
                    #if there's been an error in execution then we need to rollback everything
                    rollback transaction, err, done
            else if result
                #then the transaction has succeeded.
                _updateTransactionStatus transaction, 'Processing', 'Committed', (err) ->
                    return done(err, 'Committed')
            else
                #the transaction has (legitimately) failed
                rollback transaction, err, done




#THESE FUNCTION PROBABLY WANT TO BE REVISION "AWARE".

# should be possible to do committed.register('db', committed.db) to use these fns
exports.db =
    updateOne: (db, transactionData, collectionName, selector, values, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            collection.update selector, values, {w:1, journal: true, upsert:false, multi: false, serializeFunctions: false}, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done( null, false) #the instruction has failed
                return done(null, true)

    insert: (db, transactionData, collectionName, documents, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            collection.insert documents, {w:1, journal: true, serializeFunctions: false}, (err, objects) ->
                if err? then return done(err, null)
                return done(null, true)


exports.transaction = () ->
    transaction =
        softwareVersion: _softwareVersion
        queue: "name"
        position: 1
        startAt: new Date()
        endAt: new Date()
        enqueuedAt: new Date()
        enqueuedBy: "username"
        status: "Queued"
        errors: []
        data: {}
        instructions: []
        rollback: []