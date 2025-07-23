package com.provectus.kafka.ui.service.masking.policies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Set;

class Remove extends MaskingPolicy {

  Remove(FieldsSelector fieldsSelector) {
    super(fieldsSelector);
  }

  @Override
  public String applyToString(String str) {
    return "null";
  }

  @Override
  public ContainerNode<?> applyToJsonContainer(ContainerNode<?> node, Set<String> maskedFields) {
    return (ContainerNode<?>) removeFields(node, maskedFields);
  }

  private JsonNode removeFields(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f -> {
        String fieldName = f.getKey();
        JsonNode fieldVal = f.getValue();
        if (!fieldShouldBeMasked(fieldName)) {
          obj.set(fieldName, removeFields(fieldVal, maskedFields));
        } else {
          maskedFields.add(fieldName); // ✅ track removed field
        }
      });
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(removeFields(e, maskedFields)));
      return arr;
    }
    return node;
  }
}
