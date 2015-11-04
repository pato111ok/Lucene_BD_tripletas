package com.lucene.test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * This is test example file to explore Lucene API
 *
 * @author vishal.zanzrukia
 * @version 1.0
 */
public class LuceneTest {
    /**
     * this is index directory path where all index file will be stored which
     * lucene uses internally.
     */
    public static final File INDEX_DIRECTORY = new File("IndexDirectory");
    /**
     * Crear indexaxion a una tabla de una base de datos
     */
    public void createIndex() {
        System.out.println("-- Indexando --");
        try {
            /**
             * Seccion instanciada JDBC
             */
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            /**
             * Realizamos la coneccion a la base de datos
             */
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/scopus_db_2015_10_14", "root", "");
            Statement stmt = conn.createStatement();
            String sql = "select * from triples_prefix";
            ResultSet rs = stmt.executeQuery(sql);

            /**
             * Seccion de Lucene
             */
            Directory directory = FSDirectory.open(INDEX_DIRECTORY);

            /**
             * Definimos el analizador
             */
            Analyzer keywordAnalyzer = new KeywordAnalyzer();

            /**
             * Preparamos la congifuracion para indexWriter(Indexar la escritura)
             */
            IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LATEST, keywordAnalyzer);
            /**
             * Creamos un nuevo index en el escritorio, removemos y previsualizamos
             * los documentos indexados
             */
            writerConfig.setOpenMode(OpenMode.CREATE);
            /**
             * Optional: for better indexing performance, if you are indexing
             * many documents,<BR>
             * increase the RAM buffer. But if you do this, increase the max
             * heap size to the JVM (eg add -Xmx512m or -Xmx1g):
             */
            // writerConfig.setRAMBufferSizeMB(256.0);

            IndexWriter iWriter = new IndexWriter(directory, writerConfig);

            int count = 0;
            Document doc = null;
            Field field = null;

            /**
             * Declaro el tipo de Strings
             */
            FieldType stringType = new FieldType();
            stringType.setTokenized(true);
            stringType.setIndexed(true);

            /**
             * Looping through resultset and adding data to index file
             */
            while (rs.next()) {
                doc = new Document();

                /**
                 * adding o in document
                 */
                field = new StringField("o", rs.getString("o"), Field.Store.YES);
                doc.add(field);

                /**
                 * adding name in document
                 */
                field = new StringField("s", rs.getString("s"), Field.Store.YES);
                 doc.add(field);
                /**
                 * adding address in document
                 */
                /*field = new StringField("address", rs.getString("address"), Field.Store.YES);
                 doc.add(field);*/
                /**
                 * adding details in document
                 */
                /*field = new StringField("details", rs.getString("details"), Field.Store.YES);
                 doc.add(field);*/
                /**
                 * Adding doc to iWriter
                 */
                iWriter.addDocument(doc);
                count++;
            }

            System.out.println(count + " registros indexados");

            /**
             * Closing iWriter
             */
            iWriter.commit();
            iWriter.close();

            /**
             * Closing JDBC connection
             */
            rs.close();
            stmt.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * to search the keywords
     *
     * @param keyword
     */
    public void search(String keyword) {

        System.out.println("-- Buscando --");

        try {
            /**
             * Searching
             */
            IndexReader directoryReader = DirectoryReader.open(FSDirectory.open(INDEX_DIRECTORY));
            IndexSearcher searcher = new IndexSearcher(directoryReader);
            Analyzer keywordAnalyzer = new KeywordAnalyzer();

            /**
             * MultiFieldQueryParser is used to search multiple fields
             */
            //String[] filesToSearch = { "id", "name", "address", "details" };
            String[] filesToSearch = {"o"};
            MultiFieldQueryParser mqp = new MultiFieldQueryParser(filesToSearch, keywordAnalyzer);

            /**
             * search the given keyword
             */
            Query query = mqp.parse(keyword);
            System.out.println("query >> " + query);

            /**
             * defining the sorting on filed "objeto"
             */
            Sort nameSort = new Sort(new SortField("o", Type.STRING));

            /**
             * Correr el query
             */
            TopDocs hits = searcher.search(query, 100, nameSort);
            System.out.println("Resultados encontrados >> " + hits.totalHits);

            Document doc = null;
            for (int i = 0; i < hits.totalHits; i++) {
                /**
                 * ir al siguiente documento
                 */
                doc = searcher.doc(hits.scoreDocs[i].doc);
                //System.out.println("==========" + (i + 1) + " : Start Record=========\nId :: " + doc.get("id") + "\nName :: " + doc.get("name") + "\nDetails :: " + doc.get("details") + "\n==========End Record=========\n");
                System.out.println("==========" + (i + 1) + " : Grabacion=========\nO :: " + doc.get("o") + "\nS :: " + doc.get("s") + "\n==========Fin Grabacion=========\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * main method to check the output
     *
     * @param args
     */
    public static void main(String[] args) {

        LuceneTest obj = new LuceneTest();

        /**
         * crear el index
         */
        obj.createIndex();

        /**
         * searching simple keyword
         */
        System.out.println("================== Busqueda normal ==========================");
        obj.search("Barowski");

        /**
         * searching using wild card
         */
        System.out.println("================== Buscando usando comodin ==================");
        obj.search("au*");

        /**
         * searching using logical OR operator
         */
        System.out.println("================== Buscando usando el operator logico OR =====");
        obj.search("author OR No*");

        /**
         * searching using logical AND operator
         */
        System.out.println("================== Buscando usando el operator logico AND ====");
        obj.search("Julie AND Julie");

    }

}
