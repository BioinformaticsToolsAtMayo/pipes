package edu.mayo.pipes.bioinformatics;

/**
 * Enumeration that defines standardized CORE attributes. These will show up in
 * JSON schema-free payloads and used by downstream pipes.
 */
public enum CoreAttributes {
	/**
	 * Unique identifier (example: Accession ID).
	 */
	_id,

	/**
	 * Landmark region (example: chromosome)
	 */
	_landmark,

	/**
	 * Minimum basepair position on a chromosome.
	 */
	_minBP,

	/**
	 * Maximum basepair position on a chromosome.
	 */
	_maxBP,

	/**
	 * Chromosome strand.
	 */
	_strand,

	/**
	 * Reference allele.
	 */
	_refAllele,

	/**
	 * Alternative alleles.
	 */
	_altAlleles,
        /**
         * Tye type of the feature, e.g. gene, mRNA, contig, drug, variant ...
         */
        _type
}
