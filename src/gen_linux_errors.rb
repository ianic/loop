generic_zig = "/usr/local/zig/zig-linux-aarch64-0.11.0-dev.3286+dcc1b4fd1/lib/std/os/linux/errno/generic.zig"

error_name = nil
desc = nil
errors = []
File.open(generic_zig).each do |line|
  line = line.strip
  next if line.include?("SUCCESS")
  if line.start_with?("///")
    next if error_name
    desc = line[4..]
    error_name = desc.
                   split(":")[0].
                   gsub(".", "").
                   split('-').collect(&:capitalize).join.
                   split(' ').collect(&:capitalize).join.
                   gsub("I/o", "IO")
    next
  end
  m = /([@"\w]*) = (\d*),/.match(line)
  if m && m.length == 3
    #print ".#{m[1]} => error.#{error_name}, // #{m[2]} #{desc}\n"
    errors.push({
      :code => m[1],
      :no => m[2],
      :name => error_name,
      :desc => desc,
    })
  end
  error_name = nil
end

print "pub const ErrnoError = error{\n"
errors.each do |e|
  print "    #{e[:name]}, // #{e[:code]} = #{e[:no]} #{e[:desc]}\n"
end
print "};\n\n"

print "fn errnoError(errno: os.E) ErrnoError {\n"
print "    return switch (@intToEnum(os.E, -completion.result)) {\n"
errors.each do |e|
  print "        .#{e[:code]} => error.#{e[:name]}, // #{e[:no]} #{e[:desc]}\n"
end
print "        else => |e| return os.unexpectedErrno(e),\n"
print "    };\n}\n"
