package com.provectus.kafka.ui.service.masking.policies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Set;

class Replace extends MaskingPolicy {

  private final String replacement;

  static final String DEFAULT_REPLACEMENT = "ANONYMIZED";

  Replace(FieldsSelector fieldsSelector, String replacement) {
    super(fieldsSelector);
    this.replacement = replacement;
  }

  @Override
  public String applyToString(String str) {
    return replacement;
  }

  @Override
  public ContainerNode<?> applyToJsonContainer(ContainerNode<?> node, Set<String> maskedFields) {
    return (ContainerNode<?>) replaceFields(node, maskedFields);
  }

  private JsonNode replaceFields(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f -> {
        String fieldName = f.getKey();
        JsonNode fieldVal = f.getValue();
        if (fieldShouldBeMasked(fieldName)) {
          maskedFields.add(fieldName); // ✅ track replaced field
          obj.set(fieldName, new TextNode(replacement));
        } else {
          obj.set(fieldName, replaceFields(fieldVal, maskedFields));
        }
      });
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(replaceFields(e, maskedFields)));
      return arr;
    }
    return node;
  }
}
