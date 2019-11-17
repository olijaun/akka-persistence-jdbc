package org.jaun.akka.persistence.jdbc;

import akka.persistence.PersistentImpl;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.Tagged;
import akka.serialization.Serialization;
import akka.serialization.Serializer;
import akka.serialization.SerializerWithStringManifest;
import scala.collection.JavaConversions;
import scala.util.Try;

import java.io.NotSerializableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class Converter {

    static final String AKKA_WRITER_UUID = "akka.persistentRepr.writerUuid";
    static final String AKKA_SENDER = "akka.persistentRepr.sender";
    static final String AKKA_SERIALIZER_ID = "akka.serialization.serializerId";

    static PersistentRepr toPersistentRepr(Serialization serialization, PersistentEvent persistentEvent) {

        String writerUuid = persistentEvent.getMetadata().get(AKKA_WRITER_UUID);
        String sender = persistentEvent.getMetadata().get(AKKA_SENDER); // TODO: how to convert this back to an actor ref?

        return new PersistentImpl(toPayload(serialization, persistentEvent), persistentEvent.getSequenceNumber(), persistentEvent.getStream(), persistentEvent.getEventType(), persistentEvent.isDeleted(), null, writerUuid);
    }

    static Object toPayload(Serialization serialization, PersistentEvent persistentEvent) {

        Integer serializerId = Optional.ofNullable(persistentEvent.getMetadata().get(AKKA_SERIALIZER_ID)).map(Integer::parseInt)
                .orElseThrow(() -> new IllegalStateException("no serializer id for persistent event: " + persistentEvent));

        Try<?> deserializedEventTry = serialization.deserialize(persistentEvent.getSerializedEvent(), serializerId, persistentEvent.getEventType());

        if (deserializedEventTry.isFailure()) {
            throw new IllegalStateException(deserializedEventTry.failed().get());
        }

        return deserializedEventTry.get();
    }

    static PersistentEvent toPersistentEvent(Serialization serialization, PersistentRepr persistentRepr) {

        Object payloadObject;
        Set<String> tags;
        if (persistentRepr.payload() instanceof Tagged) {
            Tagged tagged = (Tagged) persistentRepr.payload();
            payloadObject = tagged.payload();
            tags = JavaConversions.setAsJavaSet(tagged.tags());
        } else {
            payloadObject = persistentRepr.payload();
            tags = Collections.emptySet();
        }

        Serializer serializer;
        try {
            serializer = serialization.serializerFor(payloadObject.getClass());
        } catch (NotSerializableException e) {
            throw new IllegalStateException(e);
        }

        String manifest;
        if (serializer.includeManifest()) {

            if (serializer instanceof SerializerWithStringManifest) {
                SerializerWithStringManifest serializerWithStringManifest = (SerializerWithStringManifest) serializer;
                manifest = serializerWithStringManifest.manifest(payloadObject);
            } else {
                manifest = payloadObject.getClass().getName();
            }
        } else {
            // TODO: let's see what we do here...
            manifest = payloadObject.getClass().getName();
        }

        Try<byte[]> serializedPayload = serialization.serialize(payloadObject);

        if (serializedPayload.isFailure()) {
            throw new IllegalArgumentException("serialization failed: " + serializedPayload.failed().get());
        }

        String sender = null;
        if (persistentRepr.sender() != null) {
            sender = persistentRepr.sender().path().toSerializationFormat();
        }

        HashMap<String, String> metadataMap = new HashMap<>();
        metadataMap.put(AKKA_WRITER_UUID, persistentRepr.writerUuid());
        metadataMap.put(AKKA_SENDER, sender);
        metadataMap.put(AKKA_SERIALIZER_ID, String.valueOf(serializer.identifier()));

        return PersistentEvent.builder()
                .stream(persistentRepr.persistenceId())
                .serializedEvent(serializedPayload.get())
                .sequenceNumber(persistentRepr.sequenceNr())
                .eventType(manifest)
                .tags(tags)
                .metadata(metadataMap)
                .deleted(persistentRepr.deleted()).build();
    }

}
