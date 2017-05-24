require 'pry'

class AvroTurf::SchemaStore
  attr_reader :path, :schemas

  def initialize(path: nil)
    fail ArgumentError, "missing required argument `path'" unless path
    @path = Pathname.new path
  end

  def schemas
    @schemas ||= {}
  end

  # Resolves and returns a schema.
  #
  # schema_name - The String name of the schema to resolve.
  #
  # Returns an Avro::Schema.
  def find name, namespace=nil
    fullname = Avro::Name.make_fullname name, namespace
    pathname = path.join(fullname.gsub(".", "/")).sub_ext(".avsc")

    load_schema fullname, pathname
  end

  # Loads all schema definition files in the `schemas_dir`.
  def load_schemas!
    Pathname.glob(path, "**/*.avsc").each do |pathname|
      next if pathname.directory?
      # Remove the path prefix and chop off the file extension, finally
      # Replace `/` with `.`
      fullname = pathname
        .sub_ext("")
        .to_s
        .gsub("/", ".")

      # Load and cache the schema.
      load_schema fullname, pathname
    end
  end

  protected

  def load_schema fullname, pathname
    return schemas[ fullname ] if schemas.key? fullname

    schema = parse_file pathname

    if schema.respond_to?(:fullname) && schema.fullname != fullname
      raise AvroTurf::SchemaError, "expected schema `#{ pathname }' to define type `#{ fullname }'"
    end

    schema
  rescue ::Avro::SchemaParseError => exception
    # This is a hack in order to figure out exactly which type was missing. The
    # Avro gem ought to provide this data directly.
    match = exception.to_s.match %r{"([\w\.]+)" is not a schema we know about}
    raise unless match || match.captures.empty?

    find match.captures.first

    # Re-resolve the original schema now that the dependency has been resolved.
    schemas.delete fullname

    retry
  end

  def parse_file pathname
    schema_json = JSON.parse pathname.read

    Avro::Schema.real_parse schema_json, schemas
  rescue Errno::ENOENT, Errno::ENAMETOOLONG
    raise AvroTurf::SchemaNotFoundError, "could not find Avro schema at `#{ pathname }'"
  end
end
