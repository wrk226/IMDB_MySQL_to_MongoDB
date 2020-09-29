/*
 * IMDBSQL_ToMongo.java
 *
 * Version:
 *     1.00
 *
 */

package IMDBSQLToMongo;


import java.lang.annotation.Documented;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;


/**
 * @author: Renke Wang
 */

public class IMDBSQL_ToMongo {
    private static MongoClient getClient( String u ) {
        MongoClient client = null;
        if ( u.equals( "None" ) )
            client = new MongoClient();
        else
            client = new MongoClient( new MongoClientURI( u ) );
        return client;
    }

    public static void main( String[] args ) throws SQLException {
        for(String argse:args){
            System.out.println(argse);
        }

        String mysqlUrl = args[0];
        String user = args[1];
        String pwd = args[2];
        String moUrl=args[3];
        String modb=args[4];

        Connection con = DriverManager.getConnection( mysqlUrl, user, pwd );
        Statement st = con.createStatement();
        st.setFetchSize( 100 );



        //MongoClient client = getClient( "None" );
        MongoClient client = getClient( moUrl );
        MongoDatabase db = client.getDatabase( modb );
        db.drop();

        //title,releaseYear,runtime,rating,numberOfVotes for movie

        ResultSet rs = st.executeQuery( "SELECT * FROM movie" );
        db.createCollection( "Movies" );
        MongoCollection<Document> movie = db.getCollection( "Movies" );
        List<Document> movieDoc = new ArrayList<>();
        List<Document> array = new ArrayList<>();
        int counter=0;
        while ( rs.next() ) {
            Document add = new Document();
            add.append( "_id", rs.getObject( "id" ) );
            if( rs.getObject( "title" )!=null  ){
                add.append( "title", rs.getObject( "title" ) );
            }
            if( rs.getObject( "releaseYear" )!=null  ){
                add.append( "releaseYear", rs.getObject( "releaseYear" ));
            }
            if( rs.getObject( "runtime" )!=null ){
                add.append( "runtime", rs.getObject( "runtime" ) );
            }
            if( rs.getObject( "rating" )!=null  ){
                add.append( "rating", rs.getObject( "rating" ) );
            }
            if( rs.getObject( "numberOfVotes" )!=null  ){
                add.append( "numberOfVotes", rs.getObject( "numberOfVotes" )  );
            }
            add.append( "genres",array );
            movieDoc.add( add );
            if(counter++>=10000){
                movie.insertMany(movieDoc);
                movieDoc.clear();
                counter=0;
            }
        }
        if(counter>0){
            movie.insertMany(movieDoc);
        }
        rs.close();

        //genres[] for movie
        rs = st.executeQuery( "SELECT movieid,name FROM genre join " +
                "hasgenre on id=genreid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "movieid" ));
            Document content = new Document("$push",new Document("genres",
                    rs.getObject( "name" )));
            movie.updateOne(filter,content);
        }
        rs.close();


        //name,birthYear,deathYear for people
        //remain: actor[],composer[],director[],editor[],producer[],writer[]
        rs = st.executeQuery( "SELECT * FROM person" );
        db.createCollection( "People" );
        MongoCollection<Document> person = db.getCollection( "People" );
        List<Document> personDoc = new ArrayList<>();
        counter=0;
        while ( rs.next() ) {
            Document add = new Document();
            add.append( "_id", rs.getObject( "id" ) );
            if( rs.getObject( "name" )!=null  ){
                add.append( "name", rs.getObject( "name" ) );
            }
            if( rs.getObject( "birthYear" )!=null  ){
                add.append( "birthYear", rs.getObject( "birthYear" ) );
            }
            if( rs.getObject( "deathYear" )!=null  ){
                add.append( "deathYear", rs.getObject( "deathYear" ) );
            }
            add.append( "actor",array )
                    .append( "composer",array )
                    .append( "director",array )
                    .append( "editor",array )
                    .append( "producer",array )
                    .append( "writer",array );
            personDoc.add( add );
            if(counter++>=10000){
                person.insertMany(personDoc);
                personDoc.clear();
                counter=0;
            }
        }
        if(counter>0){
            person.insertMany(personDoc);
        }
        rs.close();

        //actor[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join actedin on id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("actor",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }
        rs.close();
        //composer[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join composedby " +
                "on id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("composer",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }
        rs.close();
        //director[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join directedby " +
                "on id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("director",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }
        rs.close();
        //editor[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join editedby on" +
                " id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("editor",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }
        rs.close();
        //producer[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join producedby " +
                "on id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("producer",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }
        rs.close();
        //writer[] for people
        rs = st.executeQuery( "SELECT movieid,id FROM person join writtenby " +
                "on id=personid" );
        while ( rs.next() ) {
            Document filter = new Document("_id",rs.getObject(
                    "id" ));
            Document content = new Document("$push",new Document("writer",
                    rs.getObject( "movieid" )));
            person.updateOne(filter,content);
        }

//        //create indexes
//        MongoCollection<Document> people = db.getCollection( "People" );
//        people.createIndex( Indexes.ascending("director"));
//        people.createIndex(Indexes.ascending("writer"));


        rs.close();
        st.close();
        con.close();



    }
}
