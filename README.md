# Machine Learning Ananlysis

# Data Set

 endoscopic and histological data were collected from 287 patients 
178 patients with Crohn’s disease (CD), 
80 patients with ulcerative colitis (UC) and 
29 patients with inflammatory bowel disease unclassified(IBDU)
Validation data (35 CD and 13 UC)

https://static-content.springer.com/esm/art%3A10.1038%2Fs41598-017-02606-2/MediaObjects/41598_2017_2606_MOESM1_ESM.xls


# Unsupervised Learning

here we use Principal component analysis (PCA)
firt with 3 componet  then with 2 componets

# Supervised Learning

we use the PCA with 2 componets as new kind of feature
this hel to clean noise but many for ploting in 2D
how the algoirthms perform the casification:
*linear kernel 
*Multi Layer Perceptron with 50 Hidden_layes
*SVC with RBF kernel
*SVC with polynomial^5

the we use a Ensemble Methods of Soft Voting 
with the algoritms :
*k-nearest neighbors (k=4)
*GaussianProcess
* SVM poly5

# future work

it will be recomendt an exploratory anlysis of Neural Network with other arquitecures
as weel Sport Vector machine with RBF kernel withan optimization of parameter C & gamma
