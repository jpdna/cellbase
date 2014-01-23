package org.opencb.cellbase.lib.mongodb;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mongodb.*;
import org.opencb.cellbase.core.common.core.Gene;
import org.opencb.cellbase.core.common.variation.GenomicVariant;
import org.opencb.cellbase.core.common.variation.GenomicVariantEffect;
import org.opencb.cellbase.core.common.variation.GenomicVariantEffectPredictor;
import org.opencb.cellbase.core.lib.api.variation.VariantEffectDBAdaptor;
import org.opencb.cellbase.core.lib.dbquery.QueryOptions;
import org.opencb.cellbase.core.lib.dbquery.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.cellbase.core.common.regulatory.RegulatoryRegion;
import org.opencb.cellbase.lib.mongodb.regulatory.RegulatoryRegionMongoDBAdaptor;

/**
 * Created with IntelliJ IDEA.
 * User: imedina
 * Date: 8/28/13
 * Time: 4:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class VariantEffectMongoDBAdaptor extends MongoDBAdaptor implements VariantEffectDBAdaptor {


    public VariantEffectMongoDBAdaptor(DB db) {
        super(db);
    }

    public VariantEffectMongoDBAdaptor(DB db, String species, String version) {
        super(db, species, version);
        mongoDBCollection = db.getCollection("core");
    }


    @Override
    public QueryResult getAllConsequenceTypesByVariant(GenomicVariant variant, QueryOptions options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<QueryResult> getAllConsequenceTypesByVariantList(List<GenomicVariant> variants, QueryOptions options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public QueryResult getAllEffectsByVariant(GenomicVariant variant, QueryOptions options) {
        return getAllEffectsByVariantList(Arrays.asList(variant), options).get(0);
    }

    @Override
    public List<QueryResult> getAllEffectsByVariantList(List<GenomicVariant> variants, QueryOptions options) {
        List<QueryResult> queryResults = new ArrayList<>(variants.size());
        GenomicVariantEffectPredictor genomicVariantEffectPredictor = new GenomicVariantEffectPredictor();
        
        for (GenomicVariant genomicVariant : variants) {
            long dbTimeStart = System.currentTimeMillis();
            List<Gene> genes = getAllGenesByVariant(genomicVariant);
            List<RegulatoryRegion> regions = getAllRegulatoryRegionsByVariant(genomicVariant);
//            List<RegulatoryRegion> regions = null;
            List<GenomicVariantEffect> list = genomicVariantEffectPredictor.getAllEffectsByVariant(genomicVariant, genes, regions);
            long dbTimeEnd = System.currentTimeMillis();
            
            QueryResult queryResult = new QueryResult();
            queryResult.setDBTime((dbTimeEnd - dbTimeStart));
            queryResult.setNumResults(list.size());
            queryResult.setResult(list);

            queryResults.add(queryResult);
        }
        
        
//        List<DBObject> queries = new ArrayList<>(variants.size());
//        for (GenomicVariant genomicVariant : variants) {
//            QueryBuilder builder = QueryBuilder.start("chromosome").is(genomicVariant.getChromosome())
//                    .and("start").lessThanEquals(genomicVariant.getPosition())
//                    .and("end").greaterThanEquals(genomicVariant.getPosition());
//            queries.add(builder.get());
//        }
//
//        options = addExcludeReturnFields("transcripts.xrefs", options);
//
//        BasicDBObject returnFields = getReturnFields(options);
//        BasicDBList list = executeFind(queries.get(0), returnFields, options, db.getCollection("core"));
//        long dbTimeStart, dbTimeEnd;
//        try {
//
//
//            GenomicVariantEffectPredictor genomicVariantEffectPredictor = new GenomicVariantEffectPredictor();
//            List<Gene> regions = jsonObjectMapper.readValue(list.toString(), new TypeReference<List<Gene>>() { });
//            dbTimeStart = System.currentTimeMillis();
//            List<GenomicVariantEffect> a = genomicVariantEffectPredictor.getAllEffectsByVariant(variants.get(0), regions, null);
//            dbTimeEnd = System.currentTimeMillis();
//
//            QueryResult queryResult = new QueryResult();
//            queryResult.setDBTime((dbTimeEnd - dbTimeStart));
//            queryResult.setNumResults(list.size());
//            queryResult.setResult(a);
//
//            queryResults.add(queryResult);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        return queryResults;
    }

    @Override
    public List<Gene> getAllGenesByVariant(GenomicVariant genomicVariant) {
        List<Gene> genes = null;
        try {
            QueryBuilder builder = QueryBuilder.start("chromosome").is(genomicVariant.getChromosome())
                    .and("start").lessThanEquals(genomicVariant.getPosition())
                    .and("end").greaterThanEquals(genomicVariant.getPosition());

            // TODO Configurable species and version
            GeneMongoDBAdaptor adaptor = (GeneMongoDBAdaptor) new MongoDBAdaptorFactory().getGeneDBAdaptor("hsapiens", "v3");
            QueryOptions options = new QueryOptions();
            options = adaptor.addExcludeReturnFields("transcripts.xrefs", options);

            BasicDBObject returnFields = adaptor.getReturnFields(options);
            BasicDBList list = adaptor.executeFind(builder.get(), returnFields, options, db.getCollection("core"));
            genes = jsonObjectMapper.readValue(list.toString(), new TypeReference<List<Gene>>() { });
        } catch (JsonParseException | JsonMappingException ex) {
            Logger.getLogger(VariantEffectMongoDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(VariantEffectMongoDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return genes;
    }

    @Override
    public List<RegulatoryRegion> getAllRegulatoryRegionsByVariant(GenomicVariant genomicVariant) {
        List<RegulatoryRegion> regions = null;
        try {
            QueryBuilder builder = QueryBuilder.start("chromosome").is(genomicVariant.getChromosome())
                    .and("start").lessThanEquals(genomicVariant.getPosition())
                    .and("end").greaterThanEquals(genomicVariant.getPosition());

            RegulatoryRegionMongoDBAdaptor adaptor = (RegulatoryRegionMongoDBAdaptor) new MongoDBAdaptorFactory().getRegulatoryRegionDBAdaptor("hsapiens", "v3");
            QueryOptions options = new QueryOptions();
            options = adaptor.addExcludeReturnFields("chunkIds", options);

            BasicDBObject returnFields = adaptor.getReturnFields(options);
            BasicDBList list = adaptor.executeFind(builder.get(), returnFields, options, db.getCollection("regulatory_region"));
            regions = jsonObjectMapper.readValue(list.toString(), new TypeReference<List<RegulatoryRegion>>() { });
        } catch (JsonParseException | JsonMappingException ex) {
            Logger.getLogger(VariantEffectMongoDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(VariantEffectMongoDBAdaptor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return regions;
    }
    
   
}
