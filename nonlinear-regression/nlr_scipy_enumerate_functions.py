import numpy as np
import scipy.optimize as sp
import math as m
import sys
import itertools
import matplotlib.pyplot as plt

NUM_FEATURES = 5

""" function for extracting data """
def extract_data(filename):
    labels = []
    fvecs = []
    r = []  # runtime
    n = []  # cores/nodes
    s = []  # submit time
    m = []  # mic
    d = []  # duedate

    i = 0

    input_file = open(filename, 'r')
    for line in input_file.readlines():
        row = line.split(",")

        # get the score to assign as a label
        labels.append(float(row[NUM_FEATURES]))

        # appedn fvecs value
        fvecs.append([float(x) for x in row[0:NUM_FEATURES]])

        # get values for runtime, nodes/cores, submit time, mic, duedate value
        r.append(float(row[0]))
        n.append(float(row[1]))
        s.append(float(row[2]))
        m.append(float(row[3]))
        d.append(float(row[4]))
        i = i + 1
    input_file.close()

    # convert the array of float arrays into a numpy float array
    fvecs_np = np.matrix(fvecs).astype(np.float32).transpose()

    # convert the array of int labels into a numpy array
    labels_np = np.array(labels).astype(dtype = np.float32)
    num_labels = i

    # convert arrays of r, n, s into np arrays
    np_r = np.array(r)
    np_n = np.array(n)
    np_s = np.array(s)
    np_m = np.array(m)
    np_d = np.array(d)

    return np_r, np_n, np_s, np_m, np_d, labels_np, num_labels


""" functions for nonlinear-regression """
# arrays for ops and funcs
operators = []
append3_operators = []
append4_operators = []
append5_operators = []
functions = []

# mul operator
def _mul(x, g, h, p1, p2):
    r, n, s, m, d = x
    return (p1 * g(r)) * (p2 * h(n))

# add operator
def _add(x, g, h, p1, p2):
    r, n, s, m, d = x
    return (p1 * g(r)) + (p2 * h(n))

# div operator
def _div(x, g, h, p1, p2, epsilon = 1e-10):
    r, n, s, m, d = x
    return (p1 * g(r)) / (p2 * h(n + epsilon))

# append3_mul function
def append3_mul(x, g, h, p3):
    r, n, s, m, d = x
    return g * (p3 * h(s))

# append3_add function
def append3_add(x, g, h, p3):
    r, n, s, m, d = x
    return g + (p3 * h(s))

# append3_div function
def append3_div(x, g, h, p3, epsilon = 1e-10):
    r, n, s, m, d = x
    return (g) / (p3 * h(s + epsilon))

# append4_mul function
def append4_mul(x, g, h, p4):
    r, n, s, m, d = x
    return g * (p4 * h(d) + m)

# append4_add function
def append4_add(x, g, h, p4):
    r, n, s, m, d = x
    return g + (p4 * h(d) + m)

# append4_div function
def append4_div(x, g, h, p4, epsilon = 1e-10):
    r, n, s, m, d = x
    return g / (p4 * h(d + epsilon) + m)

# log10 function
def _log10(x, epsilon = 1e-10):
    return np.log10(x + epsilon)

# inv function
def _inv(x, epsilon = 1e-10):
    return 1.0 / (x + epsilon)

# sqrt function
def _sqrt(x):
    return np.sqrt(x)

# get id
def _id(x):
    return x

""" -------------------------------------- """

""" define the main function """
def main():
    if len(sys.argv) < 2:
        print("Missing score distribution CSV file")
        exit()
    
    runtimes, nodes, submits, mics, duedates, train_labels, num_examples = extract_data(sys.argv[1])

    # weights for training
    w = np.zeros(num_examples)
    for i in range(0, num_examples):
        w[i] = 1.0 / (runtimes[i] * nodes[i])

    functions.append(_log10)
    functions.append(_inv)
    functions.append(_sqrt)
    functions.append(_id)

    operators.append(_mul)
    operators.append(_add)
    operators.append(_div)

    append3_operators.append(append3_mul)
    append3_operators.append(append3_add)
    append3_operators.append(append3_div)

    append4_operators.append(append4_mul)
    append4_operators.append(append4_add)
    append4_operators.append(append4_div)

    # permutations = set(list(itertools.product([0,1,2,3], repeat=NUM_FEATURES-1)))
    permutations = set(list(itertools.product([0,1,2,3], repeat=NUM_FEATURES-1)))
    print(permutations)

    all_c = []
    all_score = []
    all_popt = []

    op_labels = ["*", "+", "/"]
    f_labels = ["log10", "inv", "sqrt", "id"]

    # print("op \t append_op \t perm[0] \t perm[1] \t perm[2]")
    # print("non-linear regression:")
    # image_idx = 0
    for op in range(0, 3):
        for append_op1 in range(0, 3):
            for append_op2 in range(0, 3):
                idx = 0
                for perm in permutations:
                    c = [op, append_op1, append_op2, perm[0], perm[1], perm[2], perm[3]]
                    # c = [op, append_op1, perm[0], perm[1], perm[2]]
                    all_c.append(c)

                    # print("[loop %d]: op = %s, append_op1 = %s" % (idx, op_labels[op], op_labels[append_op1]))
                    # print("%d \t %d \t %d \t %d \t %d \t %d \t %d" % (op, append_op, perm[0], perm[1], perm[2], perm[3], perm[4]))

                    # increase index for dataset: runtimes, nodes, submits, mics, duedates
                    idx += 1

                    # define the function
                    def f(x, p1, p2, p3, p4):
                        r, n, s, m, d = x
                        _f1 = operators[c[0]]((r, n, s, m, d), functions[c[3]], functions[c[4]], p1, p2)
                        _f2 = append3_operators[c[1]]((r, n, s, m, d), _f1, functions[c[5]], p3)
                        _f3 = append4_operators[c[2]]((r, n, s, m, d), _f2, functions[c[6]], p4)
                        return _f3
                
                    try:
                        popt, pcov = sp.curve_fit(f, (runtimes, nodes, submits, mics, duedates), train_labels, p0 = [0.7,0.7,0.7,0.7], sigma=w, absolute_sigma=True)
                    except RuntimeError:
                        print("Error - cannot find a set of parameters")

                    # store all popt tuples
                    all_popt.append(popt)

                    # extract parameters
                    p1 = popt[0]
                    p2 = popt[1]
                    p3 = popt[2]
                    p4 = popt[3]

                    residuals = 0.0
                    # predict_vals = []
                    for i in range(0, len(train_labels)):
                        f_values = f((runtimes[i], nodes[i], submits[i], mics[i], duedates[i]), p1, p2, p3, p4)
                        delta = np.absolute(train_labels[i] - f_values)
                        residuals += delta
                        # predict_vals.append(f_values)
                        # print("train_labels[%d] = %f - f(%d, %d, %d, %d, %d, p = [%f,%f,%f,%f,%f]) = %f - %f = %f" % (i, train_labels[i], runtimes[i], nodes[i], submits[i], mics[i], duedates[i], p1, p2, p3, p4, p5, train_labels[i], f_values, delta))
                        # print("residuals = %f" %(residuals))
                        # labels_np = np.array(labels).astype(dtype = np.float32)
                    # f_vals = np.array(predict_vals)
                    # x = np.linspace(0, 63, 64)
                    # plt.figure(figsize=(18,8))
                    # plt.plot(x, train_labels, 'o', label = "Score Distribution")
                    # plt.plot(f_vals, label = "Non-linear Regression")
                    # plt.ylim([-0.1, 0.2])
                    # plt.legend()
                    # plt.grid()
                    # plt.savefig("fig" + str(image_idx))
                    # image_idx += 1

                    score = residuals / len(train_labels)
                    all_score.append(score)
                    # print("score = %f" %(score))
                

    print("score of non-linear regression")
    for i in range(0, len(all_c)):
        max_score_index = np.argmax(all_score)
        print("%.10fx%s(runtime) %s %.10fx%s(cores) %s %.10fx%s(submit_time) %s (%.10fx%s(duedate) + mic),fitness=%.7f" % (all_popt[max_score_index][0],
            f_labels[all_c[max_score_index][3]],
            op_labels[all_c[max_score_index][0]],
            all_popt[max_score_index][1],
            f_labels[all_c[max_score_index][4]],
            op_labels[all_c[max_score_index][1]],
            all_popt[max_score_index][2],
            f_labels[all_c[max_score_index][5]],
            op_labels[all_c[max_score_index][2]],
            all_popt[max_score_index][3],
            f_labels[all_c[max_score_index][6]],
            all_score[max_score_index]))

        all_score[max_score_index] = -1.0

    # plt.show()

    return 0


""" execute the main function """
if __name__ == '__main__':
    main()