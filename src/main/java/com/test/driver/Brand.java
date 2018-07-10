package com.test.driver;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Brand implements Serializable{
  private final String externalId;
  private final String name;
  private final String logoUrl;
}
