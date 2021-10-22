package archimate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import apoc.result.VirtualRelationship;

/**
 * This function creates a derived relationship between edge nodes of the chain 
 * using ArchiMate derivation rule.
 */
public class Derive {
    static private Map<String, Integer> relationshipsStrength;
	
	@Context
	public Log log;

    public Derive() {
		relationshipsStrength = new HashMap<String, Integer>();
		relationshipsStrength.put("RC_ASSOCIATION", 1);
		relationshipsStrength.put("RC_ACCESS", 2);
		relationshipsStrength.put("RC_SERVING", 3);
		relationshipsStrength.put("RC_REALIZATION", 4);
		relationshipsStrength.put("RC_ASSIGNMENT", 5);
		relationshipsStrength.put("RC_AGGREGATION", 6);
		relationshipsStrength.put("RC_COMPOSITION", 7);
    }

    private Relationship getWeakestRelationship(Node source, Node target) {
		Iterable<Relationship> sourceRelationships = source.getRelationships(Direction.OUTGOING);
		Relationship weakestRelationship = null;
		int weakestRelationshipStrength = 8;
		// log.info("Finding weakest relationship between <%s:%s> and <%s:%s>", source.getLabels(), source.getId(), target.getLabels(), target.getId());
		for (Relationship rel : sourceRelationships) {
			// log.info(rel.toString());
			if (rel.getEndNode().equals(target) && relationshipsStrength.get(rel.getType().name()) != null) {
				if (relationshipsStrength.get(rel.getType().name()) < weakestRelationshipStrength) {
					weakestRelationship = rel;
					weakestRelationshipStrength = relationshipsStrength.get(rel.getType().name());
				}
				if (weakestRelationshipStrength == 1) {
					break;
				}
			}
		}
		// log.info("Weakest relationship between <%s> and <%s> is %s", source, target, weakestRelationship);
		return weakestRelationship;
    }

    @UserFunction
	@Description("archimate.derive(nodes :: LIST? OF NODE?) - return relationship from first node to the last throughout all in between")
    public Relationship derive(
		@Name("nodes") List<Node> nodes) {
		
		if (nodes.size() < 2) {
			log.debug("Can't derive relationship for less than 2 objects (%s were provided)", nodes.size());
			return null;
		}
		Relationship weakestRel = null;
		int weakestRelStrength = 8;
		for (int i = 1; i < nodes.size(); i++) {
			Relationship rel = getWeakestRelationship(nodes.get(i - 1), nodes.get(i));
			if (rel == null) {
				log.debug("Provided objects list %s doesn't have unidirectional solid relationships chain", nodes);
				return null;
			}
			// log.info("Evaluated relationship strength is %d, current is %d", relationshipsStrength.get(rel.getType().name()), weakestRelStrength);
			if (relationshipsStrength.get(rel.getType().name()) < weakestRelStrength) {
				weakestRel = rel;
				weakestRelStrength = relationshipsStrength.get(rel.getType().name());
			}
			// log.info("Current weakest relationship is %s", weakestRel);
		}
		Relationship result = null;
		if (weakestRel != null) {
			Node start = nodes.get(0);
			Node end = nodes.get(nodes.size() - 1);
			result = new VirtualRelationship(start, end, weakestRel.getType());
			log.debug("Returning (%s:%s)-%s->(%s:%s)", start.getProperty("name", ""), start.getLabels(), result.getType(), end.getProperty("name", ""), end.getLabels());
		} else {
			log.info("Couldn't derive relationship for %s", nodes);
		}

		return result;
    }
}
