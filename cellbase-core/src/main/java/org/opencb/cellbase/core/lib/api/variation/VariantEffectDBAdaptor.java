package org.opencb.cellbase.core.lib.api.variation;

import org.opencb.cellbase.core.common.variation.GenomicVariant;
import org.opencb.cellbase.core.lib.dbquery.QueryOptions;
import org.opencb.cellbase.core.lib.dbquery.QueryResult;

import java.util.List;
import org.opencb.cellbase.core.common.core.Gene;
import org.opencb.cellbase.core.common.regulatory.RegulatoryRegion;


public interface VariantEffectDBAdaptor {


    @Deprecated
    public QueryResult getAllConsequenceTypesByVariant(GenomicVariant variant, QueryOptions options);

    @Deprecated
    public List<QueryResult> getAllConsequenceTypesByVariantList(List<GenomicVariant> variants, QueryOptions options);


    public QueryResult getAllEffectsByVariant(GenomicVariant variant, QueryOptions options);

    public  List<QueryResult> getAllEffectsByVariantList(List<GenomicVariant> variants, QueryOptions options);
	
//	public List<GenomicVariantEffect> getAllConsequenceTypeByVariant(GenomicVariant variant);
//	
//	public List<GenomicVariantEffect> getAllConsequenceTypeByVariant(GenomicVariant variant, Set<String> excludeSet);
//
//	
//	public List<GenomicVariantEffect> getAllConsequenceTypeByVariantList(List<GenomicVariant> variants);
//
//	public List<GenomicVariantEffect> getAllConsequenceTypeByVariantList(List<GenomicVariant> variants, Set<String> excludeSet);

	
//	public Map<GenomicVariant, List<GenomicVariantEffect>> getConsequenceTypeMap(List<GenomicVariant> variants);
//
//	public Map<GenomicVariant, List<GenomicVariantEffect>> getConsequenceTypeMap(List<GenomicVariant> variants, Set<String> excludeSet);

    public List<Gene> getAllGenesByVariant(GenomicVariant variant);
    
    public List<Gene> getAllGenesByVariantList(List<GenomicVariant> variants);
    
    public List<RegulatoryRegion> getAllRegulatoryRegionsByVariant(GenomicVariant variant);
    
    
}
