using Go = import "/go.capnp";
@0xd598217bc368711c;

$Go.package("main");
$Go.import("job");

struct Event {
	# event filtered?
	filter @0 :Bool;

	# event data in bytes
	content @1 :Data;

    # key-value metadata
	meta @2 :List(Metadata);
    struct Metadata {
      key @0 :Text;
      value @1 :Text;
    }

    # workerID that worked on the task
    worker @3 :Data;
}


struct Queue {
    workerId  @0 :Data;
    queueSize @1 :UInt64;
}

interface Connection {
    putEvent @0 (eventID :Data, event :Event) -> ();
    # queue a new event

    # TODO: rename to produce?
    # not putNewEvent but simplified (no filter, worker and id)
    putNewEvent @1 (event :Event) -> (eventID :Data);
    # put new event version

	getEvent @2 (eventID :Data) -> (event :Event);
	# get an event by ID

    getJob @3 () -> (eventID :Data, ackID :Data);

	ackJob @4 (event :Event, ackID :Data) -> (acked :Bool);
	# acknowledge a job
}

interface Workflow {
	getEventAllVersions @1 (eventID :Data) -> (events :List(Event));
    # get an event by ID

    getQueue @2 (workerID :Data) -> (queue:Queue);
    # get number of queued events per worker

    getQueues @3 () -> (queues :List(Queue));
    # get number of queued events from all workers

    # TODO: getLatestEvent is obsolete? think this will be used only by the portal
    # to show the messages for tatest events
	getLatestEventID @4 (flowID :Data) -> (eventID :Data);

    # support authentication
    # return connection interface
    # that will show event / etc?
    connect @0 (flowID: Data, workerID: Data) -> (connection :Connection);
}
