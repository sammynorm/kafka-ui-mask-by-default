package com.provectus.kafka.ui.service.masking.policies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Set;

class Keep extends MaskingPolicy {

  Keep(FieldsSelector fieldsSelector) {
    super(fieldsSelector);
  }

  @Override
  public String applyToString(String str) {
    return str; 
  }

  @Override
  public ContainerNode<?> applyToJsonContainer(ContainerNode<?> node, Set<String> maskedFields) {
    return (ContainerNode<?>) keepWithFieldsCheck(node, maskedFields);
  }

  private JsonNode keepWithFieldsCheck(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f -> {
        String fieldName = f.getKey();
        JsonNode fieldVal = f.getValue();

        if (fieldShouldBeMasked(fieldName)) {
          maskedFields.add(fieldName);
          obj.set(fieldName, keepNodeRecursively(fieldVal, maskedFields));
        } else {
          obj.set(fieldName, keepWithFieldsCheck(fieldVal, maskedFields));
        }
      });
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(keepWithFieldsCheck(e, maskedFields)));
      return arr;
    }
    return node;
  }

  private JsonNode keepNodeRecursively(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f -> obj.set(f.getKey(), keepNodeRecursively(f.getValue(), maskedFields)));
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(keepNodeRecursively(e, maskedFields)));
      return arr;
    }
    return node.isTextual() ? TextNode.valueOf(node.textValue()) : node.deepCopy();
  }
}
