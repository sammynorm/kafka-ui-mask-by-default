package com.provectus.kafka.ui.service.masking.policies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

class Mask extends MaskingPolicy {

  static final List<String> DEFAULT_PATTERN = List.of("X", "x", "n", "-");

  private final UnaryOperator<String> masker;

  Mask(FieldsSelector fieldsSelector, List<String> maskingChars) {
    super(fieldsSelector);
    this.masker = createMasker(maskingChars);
  }

  @Override
  public ContainerNode<?> applyToJsonContainer(ContainerNode<?> node, Set<String> maskedFields) {
    return (ContainerNode<?>) maskWithFieldsCheck(node, maskedFields);
  }

  @Override
  public String applyToString(String str) {
    return masker.apply(str);
  }

  private static UnaryOperator<String> createMasker(List<String> maskingChars) {
    Preconditions.checkNotNull(maskingChars);
    Preconditions.checkArgument(maskingChars.size() == 4, "mask pattern should contain 4 elements");
    return input -> {
      StringBuilder sb = new StringBuilder(input.length());
      for (int i = 0; i < input.length(); i++) {
        int cp = input.codePointAt(i);
        switch (Character.getType(cp)) {
          case Character.SPACE_SEPARATOR,
              Character.LINE_SEPARATOR,
              Character.PARAGRAPH_SEPARATOR -> sb.appendCodePoint(cp); // keep separators as-is
          case Character.UPPERCASE_LETTER -> sb.append(maskingChars.get(0));
          case Character.LOWERCASE_LETTER -> sb.append(maskingChars.get(1));
          case Character.DECIMAL_DIGIT_NUMBER -> sb.append(maskingChars.get(2));
          default -> sb.append(maskingChars.get(3));
        }
      }
      return sb.toString();
    };
  }

  private JsonNode maskWithFieldsCheck(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f -> {
        String fieldName = f.getKey();
        JsonNode fieldVal = f.getValue();
        if (fieldShouldBeMasked(fieldName)) {
          maskedFields.add(fieldName);
          obj.set(fieldName, maskNodeRecursively(fieldVal, maskedFields));
        } else {
          obj.set(fieldName, maskWithFieldsCheck(fieldVal, maskedFields));
        }
      });
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(maskWithFieldsCheck(e, maskedFields)));
      return arr;
    }
    return node;
  }

  private JsonNode maskNodeRecursively(JsonNode node, Set<String> maskedFields) {
    if (node.isObject()) {
      ObjectNode obj = ((ObjectNode) node).objectNode();
      node.fields().forEachRemaining(f ->
          obj.set(f.getKey(), maskNodeRecursively(f.getValue(), maskedFields))
      );
      return obj;
    } else if (node.isArray()) {
      ArrayNode arr = ((ArrayNode) node).arrayNode(node.size());
      node.elements().forEachRemaining(e -> arr.add(maskNodeRecursively(e, maskedFields)));
      return arr;
    }
    return new TextNode(masker.apply(node.asText()));
  }
}
