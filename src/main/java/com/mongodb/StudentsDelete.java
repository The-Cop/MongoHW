/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.pull;

public class StudentsDelete {
    private static final Logger logger = LoggerFactory.getLogger("logger");

    public static void main(String[] args) throws UnknownHostException {
        MongoClient client = new MongoClient();

        MongoDatabase database = client.getDatabase("school");
        final MongoCollection<Document> students = database.getCollection("students");
        MongoCursor<Document> cursor = students.find().iterator();
        while (cursor.hasNext()) {
            Document student = cursor.next();
            List<Document> scores = (List<Document>) student.get("scores");
            Document minHomework = scores.stream()
                    .filter(s -> "homework".equalsIgnoreCase(s.getString("type")))
                    .min(Comparator.comparingDouble(s -> s.getDouble("score"))).orElse(null);
            students.updateOne(
                    //identify document
                    new Document("_id", student.get("_id")),
                    //update
                    pull("scores",
                            and(
                                    eq("type", minHomework.get("type")),
                                    eq("score", minHomework.get("score"))
                            )
                    )
            );
        }
    }

    private final static class Entry {
        ObjectId objectId;
        double score;
        long studentId;

        public Entry(ObjectId objectId, double score, long studentId) {
            this.objectId = objectId;
            this.score = score;
            this.studentId = studentId;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "objectId=" + objectId +
                    ", score=" + score +
                    ", studentId=" + studentId +
                    '}';
        }
    }
}
