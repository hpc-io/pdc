module JuliaHelper

export test_embedded_julia
export generate_attribute_occurrences
export generate_incremental_associations

using Distributions

'''
    test_embedded_julia(input::Int64, input2::Int64, intput3::Int64)

Test function for embedding Julia in C.

# Arguments
- `input::Int64`: The first input
- `input2::Int64`: The second input
- `intput3::Int64`: The third input

# Returns
- `result::Vector{Int64}`: A vector containing the inputs plus 1, 2, and 3, respectively.
'''
function test_embedded_julia(input::Int64, input2::Int64, intput3::Int64)
    result = [input, input + 1, input2 + 2, intput3 + 3]
    return result
end

'''
    generate_attribute_occurrences(num_attributes::Int64, num_objects::Int64, distribution::String, s::Float64=1.0)

Generate an array of the number of objects for each attribute, where the number of objects for each attribute is a random value.

# Arguments
- `num_attributes::Int64`: The total number of attributes
- `num_objects::Int64`: The total number of objects
- `distribution::String`: The distribution to use for generating the number of objects for each attribute. Valid values are "uniform", "pareto", "normal", and "exponential".
- `s::Float64=1.0`: The shape parameter for the distribution. This parameter is only used for the Pareto, Normal, and Exponential distributions.

# Returns
- `attribute_objects::Vector{Int64}`: An array of the number of objects for each attribute. The array length is equal to the number of attributes.
'''
function generate_attribute_occurrences(num_attributes::Int64, num_objects::Int64, distribution::String, s::Float64=1.0)
    if distribution == "uniform"
        dist = Multinomial(num_objects, fill(1.0/num_attributes, num_attributes))
        occurrences = rand(dist, 1)
    elseif distribution in ["pareto", "normal", "exponential"]
        if distribution == "pareto"
            dist = Pareto(s, 1.0)  # Pareto distribution with shape parameter s and scale 1.0
        elseif distribution == "normal"
            dist = Normal(0.0, s)
        elseif distribution == "exponential"
            dist = Exponential(s)
        end
        occurrences = rand(dist, num_attributes)
        occurrences = Int64.(round.(occurrences .* num_objects ./ sum(occurrences)))

        # Due to rounding, the sum might not be exactly num_objects. 
        # Add or subtract the difference to a random element.
        diff = num_objects - sum(occurrences)
        occurrences[rand(1:num_attributes)] += diff
    else
        error("Invalid distribution: " * distribution)
    end
    
    return sort(occurrences[:])
end

'''
    generate_incremental_associations(num_attributes::Int64, num_objects::Int64, total_groups::Int64)

Generate an array of the number of objects for each attribute, where the number of objects for each attribute is an incremental value.

# Arguments
- `num_attributes::Int64`: The total number of attributes
- `num_objects::Int64`: The total number of objects
- `num_obj_groups::Int64`: The total number of object groups

# Returns
- `attribute_objects::Vector{Int64}`: An array of the number of objects for each attribute. The array length is equal to the number of attributes.
'''
function generate_incremental_associations(num_attributes::Int64, num_objects::Int64, num_obj_groups::Int64=0)
    if num_obj_groups == 0
        num_obj_groups = num_attributes
    end
    
    # Error checking: Ensure that the number of attributes does not exceed the total number of groups
    if num_attributes > num_obj_groups
        error("Number of attributes cannot be greater than the total number of groups")
    end

    # Generate an array to store the number of objects for each attribute
    attribute_objects = Vector{Int64}(undef, num_attributes)

    # calculate the number of objects within each group
    increment_value = ceil(Int64, num_objects / num_obj_groups)

    # Assign an incremental number of objects to each attribute
    for i = 1:num_attributes
        # Ensure the number of objects does not exceed the total number of objects
        attribute_objects[i] = min(i * increment_value, num_objects)
    end

    return attribute_objects
end

end  # module MyModule