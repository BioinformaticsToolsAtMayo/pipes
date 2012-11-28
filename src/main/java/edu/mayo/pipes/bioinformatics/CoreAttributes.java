package edu.mayo.pipes.bioinformatics;

/**
 * Enumeration that defines standardized CORE attributes. These will show up in
 * JSON schema-free payloads and used by downstream pipes.
 */
public enum CoreAttributes {
	/**
	 * Accession ID.
	 */
	_accID,

	/**
	 * Chromosome.
	 */
	_chrom,

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
	_altAlleles
}
