package edu.mayo.pipes.bioinformatics.vocab;

public enum Type {
	GENE {
		public String toString() {
			return "gene";
		}		
	},
	
	VARIANT {
		public String toString() {
			return "variant";
		}		
	}
}
