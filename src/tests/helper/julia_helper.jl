using Distributions


function my_julia_func(input::Int64)
    result = [input, input + 1, input + 2, input + 3]
    return result
end

function generate_attribute_occurrences(num_attributes::Int64, num_objects::Int64, distribution::String, s::Float64=1.0)
    if distribution == "uniform"
        dist = Multinomial(num_objects, fill(1.0/num_attributes, num_attributes))
        return Int64.(rand(dist, 1))
    elseif distribution == "zipf"
        dist = Zipf(s, num_attributes)
        occurrences = Int64.(rand(dist, num_objects))
        
        # count the number of occurrences of each attribute
        attribute_counts = zeros(Int64, num_attributes)
        for i in 1:num_objects
            attribute_counts[occurrences[i]] += 1
        end
        
        return attribute_counts
    elseif distribution == "pareto"
        dist = Pareto(s, 1.0)  # Pareto distribution with shape parameter s and scale 1.0
        occurrences = Int64.(round.(occurrences .* num_objects ./ sum(occurrences)))
        
        # Due to rounding, the sum might not be exactly num_objects. 
        # Add or subtract the difference to a random element.
        diff = num_objects - sum(occurrences)
        occurrences[rand(1:num_attributes)] += diff
        
        return occurrences
    elseif distribution == "normal"
        dist = Normal(0.0, s)
        occurrences = Int64.(round.(occurrences .* num_objects ./ sum(occurrences)))
        
        # Due to rounding, the sum might not be exactly num_objects. 
        # Add or subtract the difference to a random element.
        diff = num_objects - sum(occurrences)
        occurrences[rand(1:num_attributes)] += diff
        
        return occurrences
    elseif distribution == "exponential"
        dist = Exponential(s)
        occurrences = Int64.(round.(occurrences .* num_objects ./ sum(occurrences)))
        
        # Due to rounding, the sum might not be exactly num_objects. 
        # Add or subtract the difference to a random element.
        diff = num_objects - sum(occurrences)
        occurrences[rand(1:num_attributes)] += diff
        
        return occurrences
    else
        error("Invalid distribution: " * distribution)
    end
end


function generate_incremental_associations(total_objects::Int64, total_groups::Int64, num_attributes::Int64)
    # Error checking: Ensure that the number of attributes does not exceed the total number of groups
    if num_attributes > total_groups
        error("Number of attributes cannot be greater than the total number of groups")
    end

    # Generate an array to store the number of objects for each attribute
    attribute_objects = Vector{Int64}(undef, num_attributes)
    
    # Calculate the increment value based on the total number of objects and the number of attributes
    increment_value = ceil(Int64, total_objects / num_attributes)

    # Assign an incremental number of objects to each attribute
    for i = 1:num_attributes
        # Ensure the number of objects does not exceed the total number of objects
        attribute_objects[i] = min(i * increment_value, total_objects)
    end

    return attribute_objects
end