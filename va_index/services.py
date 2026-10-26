"""VA preprocessing, final writing and online recall service boundaries."""

from .messages import TaskMessage, WriteMessage
from .regions import RegionalService, local_service
from .storage import SimulationError, stable_hash


class Preprocessor(RegionalService):
    va_only = True

    def __init__(self, config, broker, features):
        super().__init__(config.region)
        self.config, self.broker, self.features = config, broker, features

    @local_service
    def handle(self, envelope, *, outputs=None):
        if envelope.get("kind") != "task":
            raise SimulationError("Preprocessor accepts only task messages")
        message = TaskMessage.parse(envelope.get("payload"))
        snapshot = self.config.snapshot()
        if outputs is None:
            outputs = snapshot.consumer(snapshot.task_queue).get("outputs", {})
        indexes = snapshot.dataset(message.dataset)
        for index in indexes:
            if index.db_name not in outputs:
                raise SimulationError(f"No configured preprocessor output for {index.db_name}")
        groups = {}
        published = []
        for index in indexes:
            groups.setdefault(index.feature_name, []).append(index)
        for business_id in message.ids:
            for feature_name, targets in groups.items():
                embedding = self.features.compute(business_id, feature_name, message.mode)
                for index in targets:
                    # One message per physical queue. The historical list contract
                    # remains explicit; the list contains only that queue's target.
                    write = WriteMessage(
                        business_id, embedding, [index.db_name],
                        [index.db_name] if index.is_compressed else [],
                        {"task_id": message.task_id, "dataset": message.dataset,
                         "feature_name": feature_name, "mode": message.mode,
                         "attr_by_db": {index.db_name: index.extra_config.get("attr", {})}},
                    )
                    WriteMessage.parse(write.payload())
                    message_id = stable_hash([message.task_id, business_id, index.db_name])
                    self.broker.publish(outputs[index.db_name], "write",
                                        write.payload(), message_id)
                    published.append({"queue": outputs[index.db_name],
                                      "message_id": message_id})
        return {"published": published}


class Writer(RegionalService):
    def __init__(self, config, vectors, compressed):
        super().__init__(config.region)
        self.config, self.vectors, self.compressed = config, vectors, compressed

    @local_service
    def handle(self, queue, envelope):
        if envelope.get("kind") != "write":
            raise SimulationError("Writer accepts only write messages")
        message = WriteMessage.parse(envelope.get("payload"))
        snapshot = self.config.snapshot()
        # Validate every route before performing any writes.
        for db_name in message.db_list:
            index = snapshot.by_db(db_name)
            if snapshot.index_queues[db_name] != queue:
                raise SimulationError(f"Wrong queue {queue} for physical database {db_name}")
            if index.feature_name != message.context["feature_name"]:
                raise SimulationError(f"Feature does not match target {db_name}")
            dimension = snapshot.tcc["features"][index.feature_name]["dimension"]
            if len(message.embedding) != dimension:
                raise SimulationError(f"{db_name}: expected embedding dimension {dimension}")

        # Original VA assumption: a single deployment namespace is used for writes.
        namespace = snapshot.tcc["namespace"]
        writes = []
        for db_name in message.db_list:
            # Compression mode is the upstream decision carried in the message.
            # Do not re-infer it from the latest Conf while consuming old messages.
            if db_name in message.compressed_db_list:
                record_id = self.compressed.write(
                    db_name, message.id, message.embedding,
                    lambda hash_id, ids: self.vectors.upsert(
                        namespace, db_name, hash_id, message.embedding, ids),
                )
            else:
                record_id = message.id
                self.vectors.upsert(namespace, db_name, message.id, message.embedding,
                                    message.context["attr_by_db"][db_name])
            writes.append({"region": self.region, "namespace": namespace, "db_name": db_name, "id": record_id,
                           "business_id": message.id})
        return {"writes": writes}


class RecallProxy(RegionalService):
    def __init__(self, config, vectors):
        super().__init__(config.region)
        self.config, self.vectors = config, vectors

    @local_service
    def recall(self, corpus_id, embedding, *, top_k=None, return_attr=None, return_embedding=None):
        snapshot = self.config.snapshot()
        index = snapshot.index(corpus_id)
        options = {**snapshot.tcc["recall"], **index.extra_config.get("recall", {})}
        if top_k is not None:
            options["top_k"] = top_k
        if return_attr is not None:
            options["return_attr"] = return_attr
        if return_embedding is not None:
            options["return_embedding"] = return_embedding
        hits = self.vectors.recall(index.namespace, index.db_name, embedding, **options)
        return {"region": self.region, "corpusID": corpus_id, "namespace": index.namespace,
                "db_name": index.db_name, "mock_scores": True, "hits": hits}


class Forwarder(RegionalService):
    """A consumer that explicitly publishes the unchanged body to a local queue."""

    def __init__(self, broker):
        super().__init__(broker.region)
        self.broker = broker

    @local_service
    def handle(self, queue, envelope, output_queue):
        message_id = stable_hash(["forward", queue, envelope["message_id"], output_queue])
        self.broker.publish(output_queue, envelope["kind"], envelope["payload"], message_id)
        return {"published": [{"queue": output_queue, "message_id": message_id}]}
