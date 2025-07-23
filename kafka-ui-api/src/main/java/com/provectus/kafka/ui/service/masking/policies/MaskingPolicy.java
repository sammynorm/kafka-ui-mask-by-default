package com.provectus.kafka.ui.service.masking.policies;

import com.fasterxml.jackson.databind.node.ContainerNode;
import com.provectus.kafka.ui.config.ClustersProperties;
import java.util.Set;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class MaskingPolicy {


  public static MaskingPolicy create(ClustersProperties.Masking property) {
    FieldsSelector fieldsSelector = FieldsSelector.create(property);
    return switch (property.getType()) {
      case REMOVE -> new Remove(fieldsSelector);
      case REPLACE -> new Replace(
          fieldsSelector,
          property.getReplacement() == null
              ? Replace.DEFAULT_REPLACEMENT
              : property.getReplacement()
      );
      case MASK -> new Mask(
          fieldsSelector,
          property.getMaskingCharsReplacement() == null
              ? Mask.DEFAULT_PATTERN
              : property.getMaskingCharsReplacement()
      );
      case KEEP -> new Keep(fieldsSelector);
    };
  }

  //----------------------------------------------------------------

  private final FieldsSelector fieldsSelector;

  protected boolean fieldShouldBeMasked(String fieldName) {
    return fieldsSelector.shouldBeMasked(fieldName);
  }

  public abstract ContainerNode<?> applyToJsonContainer(ContainerNode<?> node, Set<String> maskedFields);

  public abstract String applyToString(String str);

}
