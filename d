@or(
    not(isInt(last(split(substring(pipeline().parameters.fileName, 0, lastIndexOf(pipeline().parameters.fileName, '.')), '_')))), 
    or(
        greaterOrEquals(length(last(split(substring(pipeline().parameters.fileName, 0, lastIndexOf(pipeline().parameters.fileName, '.')), '_'))), 4), 
        equals(last(split(substring(pipeline().parameters.fileName, 0, lastIndexOf(pipeline().parameters.fileName, '.')), '_')), '1')
    )
)
