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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

public class StudentsDelete {
    private static final Logger logger = LoggerFactory.getLogger("logger");

    public static void main(String[] args) throws UnknownHostException {
        MongoClient client = new MongoClient();

        MongoDatabase database = client.getDatabase("students");
        final MongoCollection<Document> grades = database.getCollection("grades");
        List<Document> list = grades.find()
                .filter(eq("type", "homework"))
                .sort(Sorts.ascending("student_id", "score"))
                .into(new ArrayList<Document>());

        List<Entry> el = list.stream()
                .map(l -> new Entry(l.getObjectId("_id"), l.getDouble("score"), l.getInteger("student_id")))
                .collect(Collectors.toList());
        el.forEach(l -> System.err.println(l));

        System.err.println("===================================================================================================================================");
        Map<Long, Entry> mins = new LinkedHashMap<>();
        el.forEach(l -> {
            if (!mins.containsKey(l.studentId)) {
                mins.put(l.studentId, l);
            } else if (mins.get(l.studentId).score > l.score) {
                mins.put(l.studentId, l);
            }
        });
        mins.forEach((k, v) -> System.err.println(v));

        mins.forEach((k,v)->grades.deleteOne(eq("_id",v.objectId)));
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
