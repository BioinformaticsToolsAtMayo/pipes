package edu.mayo.pipes.bioinformatics.vocab;

public enum Strand {
	FORWARD {
		public String toString() {
			return "+";
		}
	},
	
	REVERSE {
		public String toString() {
			return "-";
		}
	},

	UNKNOWN {
		public String toString() {
			return ".";
		}
	}
}