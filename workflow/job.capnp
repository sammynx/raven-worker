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

interface Workflow {
    putEvent @0 (flowID :Data, workerID :Data, eventID :Data, event :Event) -> ();
    # queue a new event

    putNewEvent @1 (flowID :Data, workerID :Data, event :Event) -> (eventID :Data);
    # put new event version

	getEvent @2 (eventID :Data) -> (event :Event);
	# get an event by ID

	getEventAllVersions @3 (eventID :Data) -> (events :List(Event));
    # get an event by ID

	getLatestEventID @4 (flowID :Data) -> (eventID :Data);

    getJob @5 (workerID :Data) -> (eventID :Data, ackID :Data, flowID :Data);

	ackJob @6 (event :Event, ackID :Data) -> (acked :Bool);
	# acknowledge a job
}
