// Data type mapping between ClickHouse and flat file formats
const dataTypeMappings = {
  // ClickHouse to Flat File mappings
  clickhouseToFlat: {
    'UInt8': 'integer',
    'UInt16': 'integer',
    'UInt32': 'integer',
    'UInt64': 'integer',
    'Int8': 'integer',
    'Int16': 'integer',
    'Int32': 'integer',
    'Int64': 'integer',
    'Float32': 'float',
    'Float64': 'float',
    'String': 'string',
    'FixedString': 'string',
    'Date': 'date',
    'DateTime': 'datetime',
    'Enum8': 'string',
    'Enum16': 'string',
    'Array': 'array',
    'Nullable': 'nullable'
  },

  // Flat File to ClickHouse mappings
  flatToClickhouse: {
    'integer': 'Int32',
    'float': 'Float64',
    'string': 'String',
    'date': 'Date',
    'datetime': 'DateTime',
    'array': 'Array(String)',
    'boolean': 'UInt8'
  }
};

// Function to convert ClickHouse type to flat file type
export const convertClickHouseToFlat = (clickhouseType) => {
  // Handle Nullable types
  if (clickhouseType.startsWith('Nullable(')) {
    const baseType = clickhouseType.slice(9, -1);
    return {
      type: dataTypeMappings.clickhouseToFlat[baseType] || 'string',
      nullable: true
    };
  }

  // Handle Array types
  if (clickhouseType.startsWith('Array(')) {
    const elementType = clickhouseType.slice(6, -1);
    return {
      type: 'array',
      elementType: dataTypeMappings.clickhouseToFlat[elementType] || 'string'
    };
  }

  return {
    type: dataTypeMappings.clickhouseToFlat[clickhouseType] || 'string',
    nullable: false
  };
};

// Function to convert flat file type to ClickHouse type
export const convertFlatToClickHouse = (flatType, nullable = false) => {
  let clickhouseType = dataTypeMappings.flatToClickhouse[flatType] || 'String';
  
  if (nullable) {
    clickhouseType = `Nullable(${clickhouseType})`;
  }
  
  return clickhouseType;
};

// Function to validate data type compatibility
export const validateDataTypeCompatibility = (sourceType, targetType) => {
  const sourceMapping = convertClickHouseToFlat(sourceType);
  const targetMapping = convertClickHouseToFlat(targetType);

  // Basic type compatibility check
  if (sourceMapping.type === targetMapping.type) {
    return true;
  }

  // Special cases for numeric types
  if (['integer', 'float'].includes(sourceMapping.type) && 
      ['integer', 'float'].includes(targetMapping.type)) {
    return true;
  }

  // String can be converted to most types
  if (sourceMapping.type === 'string') {
    return true;
  }

  return false;
};

// Function to get default value for a type
export const getDefaultValue = (type) => {
  switch (type) {
    case 'integer':
    case 'float':
      return 0;
    case 'string':
      return '';
    case 'date':
    case 'datetime':
      return new Date().toISOString();
    case 'boolean':
      return false;
    case 'array':
      return [];
    default:
      return null;
  }
};

export default {
  convertClickHouseToFlat,
  convertFlatToClickHouse,
  validateDataTypeCompatibility,
  getDefaultValue
}; 