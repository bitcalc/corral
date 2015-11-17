﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Boogie;
using System.Diagnostics;
using cba.Util;

namespace ProofMinimization
{
    // Set of template annotations implemented as a indexed set of clauses, i.e., an
    // indexed CNF formula, i.e., a CNF formula thought of as a list. 
    class TemplateAnnotations
    {
        List<double> cst = null;
        List<List<Expr>> icnf;

        // Constructors
        public TemplateAnnotations(List<Expr> cnfs)
        {
            if (cnfs.Count == 0)
            {
                icnf = new List<List<Expr>>();
            }
            else
            {
                //Debug.Assert(cnfs.All(exp => SimplifyExpr.IsCleanCNF(exp)));
                Expr cnf = SimplifyExpr.Reduce(cnfs, BinaryOperator.Opcode.And);
                icnf = makeItIndexed(cnf);
            }
        }

        public TemplateAnnotations(Expr cnf)
        {
            //Debug.Assert(SimplifyExpr.IsCleanCNF(cnf));
            icnf = makeItIndexed(cnf);
        }

        public TemplateAnnotations(List<List<Expr>> icnf)
        {
            //Debug.Assert(icnf.All(exp => (exp.All(e => SimplifyExpr.IsCleanCNF(e)))));
            this.icnf = icnf; 
        }


        static List<List<Expr>> makeItIndexed(Expr cnf)
        {
            List<List<Expr>> indexedCnf = new List<List<Expr>>();
            List<Expr> conjucts = SimplifyExpr.GetExprConjunctions(cnf);
            HashSet<string> uniqueConjucts = new HashSet<string>();
            for (int i = 0; i < conjucts.Count; i++)
            {
                // Here we don't take into account the conjucts that are 
                // exactly the same. TODO: can we somehow recognize two
                // equivalent templates as well as those that are tautologies?
                // This just looks for string equivalence.
                if (!uniqueConjucts.Contains(conjucts[i].ToString()))
                {
                    uniqueConjucts.Add(conjucts[i].ToString());
                    // Preserves order of disjuncts.
                    indexedCnf.Add(SimplifyExpr.GetExprDisjuncts(conjucts[i]));
                }
            }
            return indexedCnf;
        }


        public int ClauseCount()
        {
            return icnf.Count;
        }

        public List<Expr> GetClause(int i)
        {
            return icnf[i];
        }

        public TemplateAnnotations DeepCopy()
        {
            List<List<Expr>> clauses = new List<List<Expr>>();
            for (int i = 0; i < icnf.Count; i++)
            {
                List<Expr> clause = new List<Expr>();
                for (int j = 0; j < icnf[i].Count; j++)
                {
                    clause.Add(icnf[i][j]);
                }
   
                clauses.Add(clause);
            }

            var t = new TemplateAnnotations(clauses);
            t.SetCost(Cost());
            return t; 
        }

        public override string ToString()
        {
            string str = "{";
            for (int i = 0; i < icnf.Count; i++)
            {
                string clstr = "";
                HashSet<Expr> clause = new HashSet<Expr>();
                foreach (var lit in icnf[i])
                {
                    clstr += lit + "\t";
                }
                str += "\r\n\t" + clstr;
            }
            return str + "\r\n}";    
        }

        public Expr ToCnfExpression()
        {
            List<Expr> conjucts = new List<Expr>();
            for (int i = 0; i < icnf.Count; i++)
            {
                var clause = icnf[i];
                if (clause.Count != 0)
                {
                    // Order preserving.
                    var clauseExpr = SimplifyExpr.Reduce(clause, BinaryOperator.Opcode.Or);
                    conjucts.Add(clauseExpr);
                }
            }

            if (conjucts.Count == 0)
            {
                return Expr.True;
            } 
            else
            {
                return SimplifyExpr.Reduce(conjucts, BinaryOperator.Opcode.And);
            }
        }

        public int ClauseMaxSize() {
            int ms = 0;
            foreach (var clause in icnf) 
            {
                ms = clause.Count > ms? clause.Count : ms;
            }
            return ms;
        }

        public List<double> Cost()
        {
            return cst;
        }

        public void SetCost(List<double> c)
        {
            this.cst = c;
        }
    }


    // Lazy iterator over k=1 simplified annotation sets of a given set.
    class SimplifiedAnnotsIterator
    {
        TemplateAnnotations annots;
        MinimizerData mdata;

        // iteration state
        int clauseIndex = 0;
        int simplSizeIndex = 0;
        // size of simplified clause.
        List<int> simplificationSizes = new List<int>();

        // binary mask for subset enumeration.
        int mask = -1;

        public SimplifiedAnnotsIterator(TemplateAnnotations annots, MinimizerData mdata)
        {
            this.mdata = mdata;
            this.annots = annots;

            for (int i = 0; i <= this.annots.ClauseMaxSize(); i++)
            {
                simplificationSizes.Add(i);
            }
        }

        // Gimme the next simplified annotation set.
        public TemplateAnnotations next()
        {
            // True has no simplified versions. 
            if (annots.ToCnfExpression() == Expr.True)
            {
                return null;
            }

            // If we simplified for all simpflification sizes, we are done.
            if (simplSizeIndex >= simplificationSizes.Count)
            {
                return null;
            }

            // If we finished simplifying all clauses for the current simplification size.
            // NOTE: clauseIndex goes from 0 to annots.ClauseCount() - 1
            if (clauseIndex >= annots.ClauseCount())
            {
                clauseIndex = 0;
                mask = -1;
                simplSizeIndex++;
                return next();
            }
                
            // If we enlisted all subsets of the current clause. This also covers the case
            // when simplification size is bigger than the clause.
            var subset = nextSubset();
            if (subset == null) 
            {
                clauseIndex++;
                mask = -1;
                return next();
            }
            else
            {
                return createSimplifiedFormula(subset, clauseIndex);
            }
        }


        TemplateAnnotations createSimplifiedFormula(HashSet<int> subset, int clIndex)
        {
            TemplateAnnotations canns = annots.DeepCopy();
            var clause = canns.GetClause(clIndex);
            List<Expr> newClause = new List<Expr>();
            for (int i = 0; i < clause.Count; i++)
            {
                if (subset.Contains(i))
                {
                    newClause.Add(clause[i]);
                }
            }

            // Save new simplified annotations.
            saveSimplifiedAnnotation(clause, newClause, subset);

            clause.Clear();
            clause.AddRange(newClause);
            return canns;
        }


        void saveSimplifiedAnnotation(List<Expr> oldClause, List<Expr> newClause, HashSet<int> subset)
        {
            if (newClause.Count() == 0)
            {
                return;
            }

            Expr oldAnnot = SimplifyExpr.Reduce(oldClause, BinaryOperator.Opcode.Or);
            Expr newAnnot = SimplifyExpr.Reduce(newClause, BinaryOperator.Opcode.Or);

            if (!mdata.annotsToOrigins.ContainsKey(oldAnnot.ToString()))
            {
                Console.WriteLine("ERROR: annotation not saved to origins " + oldAnnot.ToString());
                return;
            }

            // foreach file were oldAnnotation appears
            foreach (var file in mdata.annotsToOrigins[oldAnnot.ToString()].Keys)
            {
                var procsData = mdata.annotsToOrigins[oldAnnot.ToString()][file];
                // foreach function in the file that has oldAnnotation as an anottation
                foreach (var proc in procsData.Keys)
                {
                    List<Expr> exprs = procsData[proc];
                    foreach (Expr expr in exprs)
                    {
                        List<Expr> newLits = new List<Expr>();
                        var lits = SimplifyExpr.GetExprDisjuncts(expr);
                        for (int i = 0; i < lits.Count(); i++)
                        {
                            if (subset.Contains(i))
                            {
                                newLits.Add(lits[i]);
                            }
                        }

                        Expr newExpr = SimplifyExpr.Reduce(newLits, BinaryOperator.Opcode.Or);

                        if (!mdata.annotsToOrigins.ContainsKey(newAnnot.ToString()))
                        {
                            mdata.annotsToOrigins[newAnnot.ToString()] = new Dictionary<string, Dictionary<string, List<Expr>>>();
                        }

                        if (!mdata.annotsToOrigins[newAnnot.ToString()].ContainsKey(file))
                        {
                            mdata.annotsToOrigins[newAnnot.ToString()][file] = new Dictionary<string, List<Expr>>();
                        }

                        if (!mdata.annotsToOrigins[newAnnot.ToString()][file].ContainsKey(proc))
                        {
                            mdata.annotsToOrigins[newAnnot.ToString()][file][proc] = new List<Expr>();
                        }

                        List<Expr> sexprs = mdata.annotsToOrigins[newAnnot.ToString()][file][proc];
                        HashSet<string> exprStrings = new HashSet<string>();
                        sexprs.Iter(e => exprStrings.Add(e.ToString()));

                        if (!exprStrings.Contains(newExpr.ToString()))
                        {
                            sexprs.Add(newExpr);
                        }
                    }
                }
            }
        }

        HashSet<int> nextSubset()
        {
            int simplifiedSize = simplificationSizes[simplSizeIndex];
            // This is probably not even necessary, but can save some time.
            // Simplified size always has to be strictly less than the clause size.
            if (simplifiedSize >= annots.GetClause(clauseIndex).Count)
            {
                return null;
            }

            while (true)
            {
                mask++;
                var cl = annots.GetClause(clauseIndex);
                // We are interested in strict subsets.
                if (mask >= (Math.Pow(2, cl.Count) - 1))
                {
                    break;
                }

                // Make mask a char array and add the leading 0s.
                var bits = Convert.ToString(mask, 2).ToCharArray();
                while (bits.Length < cl.Count)
                {
                    var lbits = bits.ToList();
                    lbits.Insert(0, '0');
                    bits = lbits.ToArray<Char>();
                }

                // Collect the 1 bit indices, they encode our subset.
                HashSet<int> subset = new HashSet<int>();
                for (int i = 0; i < bits.Length; i++)
                {
                    if (bits[i] == '1')
                    {
                        subset.Add(i);
                    }
                }

                // If the subset size matches our simplification size,
                // we have a simplified clause data: return it.
                if (subset.Count == simplifiedSize)
                {                    
                    return subset;
                }
            }

            return null;
        }
    }


    class K1BreadthMinimizer : Minimizer
    {
        //int call = 0;

        int sdvCalls = 0;
        int cacheHits = 0;

        static Dictionary<string, Dictionary<string, List<double>>> costCache = new Dictionary<string, Dictionary<string, List<double>>>();
        static Dictionary<string, Dictionary<string, int>> costLimitCache = new Dictionary<string, Dictionary<string, int>>();
        double pbalance = 0.5;

        static string ART_VAR_PREFIX = "ART_ZP_";

        int houdiniTemplateLimit = 30;

        // Tells us what algorithm should we use, the base one or the advanced one.
        bool baseTechnique;

        public K1BreadthMinimizer(MinimizerData mdata, bool baseTechnique) : base(mdata)
        {
            this.baseTechnique = baseTechnique;


            // Just to create a file from scratch.
            string fname = "k1-trace-" + (baseTechnique ? "base" : "redef") + ".txt";
            try
            {
                System.IO.StreamWriter file = new System.IO.StreamWriter(fname, false);
                file.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("WARNING: logger creation failed --- {0}", e.Message);
            }
        }

        // Log messages.
        private void log(String message)
        {
            string fname = "k1-trace-" + (baseTechnique? "base": "redef") + ".txt";
            try
            {
                System.IO.StreamWriter file = new System.IO.StreamWriter(fname, true);
                file.WriteLine(message);
                file.Close();
            } catch(Exception e)
            {
                Console.WriteLine("WARNING: logger failed for --- {0}", e.Message);
            }
        }


        public override HashSet<int> FindMin(out Dictionary<int, int> templateToPerfDelta)
        {
            sdvCalls = 0;
            cacheHits = 0;

            templateToPerfDelta = new Dictionary<int, int>();
            if (baseTechnique)
            {
                return FindMinBase(out templateToPerfDelta);
            }
            else
            {
                return FindMinRedef(out templateToPerfDelta);
            }
        }





        HashSet<int> FindMinRedef(out Dictionary<int, int> templateToPerfDelta)
        {
            Dictionary<string, TemplateAnnotations> minTemplates = new Dictionary<string, TemplateAnnotations>();

            var files = mdata.fileToProg.Keys.ToList();
            // Order by code size increasingly.
            files = files.OrderBy(x => mdata.fileToProg[x].getProgram().Procedures.Count()).ToList();
            //files.Reverse();

            for (int i = 0; i < files.Count; i++)
            {
                var file = files[i];
                log("\r\n\r\nWorking on file (" + (i + 1) + ") :" + file);
                log("The number of functions in the program is " + mdata.fileToProg[file].getProgram().Procedures.Count());

                log("Checking for minimal template in existing results...");
                for (int j = 0; j < i; j++)
                {
                    var f = files[j];
                    log("Checking existing result of " + f);
                    var t = minTemplates[f];
                    try
                    {
                        if (isPotentialMinimal(f, t, file) && isMinimalTemplate(file, t))
                        {
                            minTemplates[file] = t;
                            log("Found minimal template in existing results:" + t.ToString());
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        log(string.Format("ERROR: Existing minimality check failed {0} {1}", f, e.Message));
                    }
                }

                if (minTemplates.ContainsKey(file)) continue;

                log("Computing my own minimal template...");
                try
                {
                    log("\r\n\r\nComputing minimal template for file :" + file);
                    var minTemplate = computeMinimalTemplate(file);
                    minTemplates[file] = minTemplate;
                    log("Done computing minimal tempalte.");
                }
                catch (Exception e)
                {
                    log(string.Format("ERROR: Minimality computation failed {0} {1}", file, e.Message));
                }
            }

            log((new HashSet<TemplateAnnotations>(minTemplates.Values.ToList()).Count) + " different minimal templates computed.");

            TemplateAnnotations C = new TemplateAnnotations(new List<Expr>());
            int index = 0;
            while (index < files.Count)
            {
                var r = getSubRepositoryUnion(C, files, minTemplates, index);
                C = r.Item1;
                index = r.Item2;
                log("Template union C is of size " + C.ClauseCount());
                log("Subrepository index ending in " + index);
                C = computeMinimalUnionTemplate(C);
            }

            var annots = SimplifyExpr.GetExprConjunctions(C.ToCnfExpression());
            foreach (var annot in annots)
            {
                GatherAllVariables allv = new GatherAllVariables();
                allv.Visit(annot);
                HashSet<string> allvars = allv.variables;

                string prefix = "";
                // Loop annotation is that annotation that can have global but
                // (1) it should not have any general formal ins and outs and (2)
                // it should have loop variables starting with "v_loop".
                if (allvars.All(v => !v.StartsWith("v_fin") && !v.StartsWith("v_fout")) &&
                    !allvars.All(v => !v.StartsWith("v_loop")))
                {
                    prefix = "{:loop} ";
                }
                Console.WriteLine("Additional contact required: {0}", prefix + annot.ToString());
                log(string.Format("Additional contact required: {0}", prefix + annot.ToString()));
            }

            log("\r\n");
            log("Total number of SDV calls: " + sdvCalls);
            log("Total number of cache hits: " + cacheHits);
            log("MINIMIZATION FINISHED.");
            //TODO: this is currently bogus as it used only for Akash's debugging.
            templateToPerfDelta = new Dictionary<int, int>();
            return new HashSet<int>();
        }


        Tuple<TemplateAnnotations, int> getSubRepositoryUnion(TemplateAnnotations C, List<string> fNames,
                                                               Dictionary<string, TemplateAnnotations> minTemplates, int index)
        {
            Dictionary<string, Expr> annots = new Dictionary<string, Expr>();
            var cannots = SimplifyExpr.GetExprConjunctions(C.ToCnfExpression());
            foreach (var cannot in cannots)
            {
                if (cannot == Expr.True) continue;

                var s = cannot.ToString();
                if (!annots.ContainsKey(s))
                {
                    annots[s] = cannot;
                }
            }

            bool advanced = false;
            for (; index < fNames.Count; index++)
            {
                if (annots.Count >= houdiniTemplateLimit && advanced)
                {
                    break;
                }

                var fname = fNames[index];
                TemplateAnnotations ts = minTemplates[fname];
                var ants = SimplifyExpr.GetExprConjunctions(ts.ToCnfExpression());
                foreach (var ant in ants)
                {
                    if (!annots.ContainsKey(ant.ToString()) && ant != Expr.True)
                    {
                        annots[ant.ToString()] = ant;
                    }
                }
                advanced = true;
            }

            return new Tuple<TemplateAnnotations, int>(new TemplateAnnotations(annots.Values.ToList()), index);
        }


        TemplateAnnotations computeMinimalUnionTemplate(TemplateAnnotations template)
        {
            log("Computing best costs...");
            Dictionary<string, List<double>> bestCost = new Dictionary<string, List<double>>();
            var files = mdata.fileToProg.Keys.ToList();
            for (int i = 0; i < files.Count; i++)
            {
                var file = files[i];
                try
                {
                    var cost = getTemplateCost(file, mdata.fileToProg[file], template);
                    if (cost == null)
                    {
                        log("WARNING: initial cost for union template failed: " + file);
                    }

                    bestCost[file] = cost;
                }
                catch (Exception e)
                {
                    log(string.Format("ERROR: computing initial union cost failed {0} {1}", file, e.Message));
                }
            }

            log("Done computing best costs...");
            TemplateAnnotations bestTemplate = template;

            while (true)
            {
                SimplifiedAnnotsIterator iter = new SimplifiedAnnotsIterator(bestTemplate, mdata);
                bool b = false;

                log("Computing the minimal union template...");
                log("Currently best union template " + bestTemplate.ToString());
                TemplateAnnotations simple;
                while ((simple = iter.next()) != null)
                {
                    log("Considering simpler union template " + simple.ToString());

                    Dictionary<string, List<double>> newCost = new Dictionary<string, List<double>>();

                    bool hit = true;
                    foreach (var file in bestCost.Keys)
                    {
                        try
                        {
                            log("Computing cost for " + file);

                            // Set the Corral limit to 1/pbalance times of the cost you are
                            // trying to beat. Because everything above that limit is surely worse.
                            var tCost = bestCost[file];
                            int limit = tCost != null && tCost.Count > 0 ? (int)((1 / pbalance) * tCost[0]) + 1 : 0;
                            log("Setting Corral limit to " + limit);

                            var nCost = getTemplateCost(file, mdata.fileToProg[file], simple, limit);
                            newCost[file] = nCost;
                            log("Cost is " + (nCost == null ? "null" : string.Join(", ", nCost)));

                            if (!costCompare(bestCost[file], nCost))
                            {
                                hit = false;
                                break;
                            }
                        }
                        catch (Exception e)
                        {
                            log(string.Format("ERROR: computing union cost failed {0} {1}", file, e.Message));
                        }
                    }

                    if (hit)
                    {
                        log("Found better union template.");
                        bestCost = newCost;
                        bestTemplate = simple;
                        b = true;
                        break;
                    }
                }

                if (!b)
                {
                    log("Found the minimal union template!");
                    break;
                }
            }
            return bestTemplate;
        }


        bool isPotentialMinimal(string oldFile, TemplateAnnotations minTemplate, string newFile)
        {
            log("Checking for minimality compliance...");

            var fileTempIds = mdata.fileToTempIds[newFile];
            List<Expr> templates = new List<Expr>();
            foreach (var tid in fileTempIds)
            {
                var template = mdata.tempIdToExpr[tid];
                templates.Add(template);
            }

            TemplateAnnotations maxTemplate = new TemplateAnnotations(templates);
            
            // First check wether maxTemplate ->* minTemplate.
            for (int i = 0; i < minTemplate.ClauseCount(); i++)
            {
                var clause = new HashSet<string>();
                minTemplate.GetClause(i).Iter(l => clause.Add(l.ToString()));
                bool hit = false;
                for (int j = 0; j < maxTemplate.ClauseCount(); j++)
                {
                    var bclause = new HashSet<string>();
                    maxTemplate.GetClause(j).Iter(l => bclause.Add(l.ToString()));
                    if (clause.IsSubsetOf(bclause))
                    {
                        hit = true;
                        break;
                    }
                }
                if (!hit)
                {
                    log("Refusing minimality check since minimal template has nothing in common with the new program.");
                    return false;
                }
            }

            //List<double> oldCost = getTemplateCost(oldFile, mdata.fileToProg[oldFile], minTemplate);
            //List<double> newCost = getTemplateCost(newFile, mdata.fileToProg[newFile], minTemplate);

            //if (oldCost == null || newCost == null)
            //{
            //    log("Refusing minimality check since minimal template performs bad some of the given programs.");
            //    return false;
            //} 

            //if (oldCost.Count == 0 || newCost.Count == 0)
            //{
            //    throw new Exception("Cost was an empty list. This should not happen.");
            //}

            //double nprocs = newCost[0];
            //double oprocs = oldCost[0];

            //// If the minimal template performs an order of magnitude worse than for the
            //// original template, than it is probably not very useful for this program.
            //if (nprocs >= 100 * oprocs)
            //{
            //    log(string.Format("Refusing minimality check since minimal template performs bad for the new program {0} {1}.", nprocs, oprocs));
            //    return false;
            //}

            return true;
        }






        HashSet<int> FindMinBase(out Dictionary<int, int> templateToPerfDelta)
        {
            Dictionary<string, TemplateAnnotations> minTemplates = new Dictionary<string, TemplateAnnotations>();

            var files = mdata.fileToProg.Keys.ToList();
            for (int i = 0; i < files.Count; i++)
            {
                var file = files[i];
                log("\r\n\r\nWorking on file (" + (i+1) + ") :" + file);

                log("Checking for minimal template in existing results.");
                foreach (var f in minTemplates.Keys)
                {
                    log("Checking existing result for " + f);
                    var t = minTemplates[f];
                    try
                    {
                        if (isMinimalTemplate(file, t))
                        {
                            minTemplates[file] = t;
                            log("Found minimal template in existing results:" + t.ToString());
                            break;
                        } else
                        {
                            log("Not a minimal existing template.");
                        }
                    }
                    catch (Exception e)
                    {
                        log(string.Format("ERROR: Existing minimality check failed {0} {1}. Investigate!", f, e.Message));
                    }
                }

                // Basically, if the previous loop ended in a break.
                if (minTemplates.ContainsKey(file)) continue;

                log("\r\nNo minimal template found in current results. Computing my own minimal.");

                try {
                    var minTemplate = computeMinimalTemplate(file);
                    minTemplates[file] = minTemplate;
                    
                    log("Now updating previous results...");
                    List<string> keys = minTemplates.Keys.ToList();
                    for (int k = 0; k < keys.Count; k++)
                    {
                        var f = keys[k];
                        if (f == file) continue;

                        try
                        {
                            log(string.Format("Updating {0} ...", f));
                            if (isMinimalTemplate(f, minTemplate))
                            {
                                minTemplates[f] = minTemplate;
                                log("Updated!");
                            }
                            else
                            {
                                log("No need for updating.");
                            }
                        }
                        catch (Exception e)
                        {
                            log(string.Format("ERROR: Minimality check failed {0}: {1}.", f, e.Message));
                        }
                    }
                    log("Done updating!");
                } catch (Exception e)
                {
                    log(string.Format("ERROR: Something went wrong with program {0}: {1}", file, e.Message));
                }
                log("");
            }


            log((new HashSet<TemplateAnnotations>(minTemplates.Values.ToList()).Count) + " different minimal templates computed.");

            log("\r\nPRINTING MINIMAL TEMPLATE ANNOTATIONS");
            HashSet<string> uniqueAnnots = new HashSet<string>();
            foreach (var k in minTemplates.Keys)
            {
                var t = minTemplates[k];
                log(t.ToString());
                List<Expr> annots = SimplifyExpr.GetExprConjunctions(t.ToCnfExpression());
                foreach (var annot in annots)
                {
                    uniqueAnnots.Add(annot.ToString());
                }        
            }
            foreach (var annotStr in uniqueAnnots)
            {
                log(string.Format("Additional contract required: {0}", annotStr));
                Console.WriteLine("Additional contract required: {0}", annotStr);
            }

            log("\r\n");
            log("Total number of SDV calls: " + sdvCalls);
            log("Total number of cache hits: " + cacheHits);
            log("MINIMIZATION FINISHED.");

            //TODO: this is currently bogus as it used only for Akash's debugging.
            templateToPerfDelta = new Dictionary<int,int>();
            return new HashSet<int>();
        }


        TemplateAnnotations computeMinimalTemplate(string file)
        {
            var program = mdata.fileToProg[file];
            var fileTempIds = mdata.fileToTempIds[file];

            log(string.Format("Found the following templates for program {0}:", file));
            List<Expr> templates = new List<Expr>();
            foreach (var tid in fileTempIds)
            {
                log(tid.ToString());
                var template = mdata.tempIdToExpr[tid];
                log(string.Format("\t{0}", template.ToString()));
                templates.Add(template);
            }

            log(string.Format("\nCreating indexed template {0}", templates.Count));
            TemplateAnnotations icnf = new TemplateAnnotations(templates);
            
            TemplateAnnotations bestTemplate = icnf;
            log("Creating initial cost...");
            bestTemplate.SetCost(getTemplateCost(file, program, icnf)); 
            log(string.Format("Initial cost constructed: {0}", bestTemplate.Cost() == null ? "null" : string.Join(", ",  bestTemplate.Cost())));

            if (bestTemplate.Cost() == null)
            {
                throw new Exception("ERROR: initial template does not verify the program!");
            }

            while (true)
            {
                //call++;
                log(string.Format("Currently best template: \r\n{0}", bestTemplate.ToString()));
                 
                var t = getBetterTemplate(file, bestTemplate);
                if (t == null)
                {
                    break;
                }
                else
                {
                    log("Best template just changed.\n");
                    bestTemplate = t;
                }
            }

            log(string.Format("COST: {0} \r\nMINIMAL TEMPLATE: {1}\r\n", bestTemplate.Cost() == null? "null": string.Join(", ", bestTemplate.Cost()), bestTemplate.ToString()));
            return bestTemplate;
        }


        bool isMinimalTemplate(string file, TemplateAnnotations template)
        {
            var program = mdata.fileToProg[file];
            var t = template.DeepCopy();
            log("Creating initial cost...");
            t.SetCost(getTemplateCost(file, program, t));

            if (t.Cost() == null)
            {
                log("Template does not even verify the program.");
                return false;
            }

            log(string.Format("Initial cost constructed: {0}", t.Cost() == null? "null": string.Join(", ", t.Cost())));         
            var better = getBetterTemplate(file, t);
            if (better == null)
            {
                return true;
            }
            return false;
        }

        List<double> getTemplateCost(string file, ProgTransformation.PersistentProgram program, TemplateAnnotations template, int limit = 0)
        {
            var insts = instantiateTemplate(file, template, program);
            return computeCost(file, program, insts, limit);
        }

        TemplateAnnotations getBetterTemplate(string file, TemplateAnnotations template)
        {
            var program = mdata.fileToProg[file];
            log("Creating k1 simplified templates iterator...");
            SimplifiedAnnotsIterator iter = new SimplifiedAnnotsIterator(template, mdata);

            TemplateAnnotations simple;
            var tCost = template.Cost();
            while ((simple = iter.next()) != null)
            {
                log(string.Format("Checking subtemplate: {0}", simple.ToString()));

                log("Computing instantiations...");
                var insts = instantiateTemplate(file, simple, program);

                // Set the Corral limit to 1/pbalance times of the cost you are
                // trying to beat. Because everything above that limit is surely worse.
                int limit = tCost != null && tCost.Count > 0 ? (int)((1 / pbalance) * tCost[0]) + 1 : 0;
                log("Running Corral with limit set to " + limit);
                List<double> cost = computeCost(file, program, insts, limit);
                simple.SetCost(cost);

                log(string.Format("Cost is {0}", (cost == null? "null": string.Join(", ", cost))));

                if (costCompare(tCost, cost))
                {
                    log("New cost is better.");
                    return simple;
                }
            }

            return null;
        }


        bool costCompare(List<double> c1, List<double> c2)
        {
            if (c1 == null && c2 == null)
            {
                return false;
            } else if (c1 == null)
            {
                return true;
            } else if (c2 == null)
            {
                return false;
            }

            var scales = new List<double> { pbalance };
            int smindex = c1.Count < c2.Count ? c1.Count: c2.Count;
            for(int i = 0; i < smindex; i++)
            {
                var scale = i < scales.Count ? scales[i] : 1;
                var c_2 = scale * c2[i];
                if (c_2 < c1[i])
                {
                    return true;
                } else if (c1[i] < c_2)
                {
                    return false;
                }
            }

            return false;
        }

        List<double> computeCost(string file, ProgTransformation.PersistentProgram program, Dictionary<Procedure, List<Expr>> instantiation, int limit = 0)
        {
            var allconstants = new Dictionary<string, Constant>();
            var prog = program.getProgram();
            prog.TopLevelDeclarations.OfType<Constant>()
                .Where(c => QKeyValue.FindBoolAttribute(c.Attributes, "existential"))
                .Iter(c => allconstants.Add(c.Name, c));
            MinControl.DropConstants(prog, new HashSet<string>(allconstants.Keys));
            //cba.Util.BoogieUtil.PrintProgram(prog, "interim0_" + call + ".bpl");
            
            int instCnt = 0;

            string instantiationKey = "";
            List<Procedure> procedures = instantiation.Keys.ToList();
            // Unique enumeration of procedures.
            procedures.Sort((x, y) => string.Compare(x.Name, y.Name));
            for (int i = 0; i < procedures.Count; i++)
            {
                var proc = procedures[i];
                var procedure = prog.FindProcedure(proc.Name);
                
                List<Expr> procInsts = instantiation[proc];
                // Unique enumeration of template instantiations.
                procInsts.Sort((x, y) => string.Compare(x.ToString(), y.ToString()));
                for (int j = 0; j < procInsts.Count; j++)
                {
                    var expr = procInsts[j];

                    // Create (deterministically) identifier
                    string ident = createIdentifier(proc.Name, expr.ToString(), i, j);
                    instantiationKey += "-" + ident;

                    var tident = new TypedIdent(Token.NoToken, ident, Microsoft.Boogie.BasicType.Bool);
                    Constant c = new Constant(Token.NoToken, tident, false);
                    c.AddAttribute("existential", new object[1] { Microsoft.Boogie.Expr.True });
                    prog.AddTopLevelDeclaration(c);

                    var identExp = new IdentifierExpr(Token.NoToken, tident.Name, tident.Type);
                    var impl = new NAryExpr(Token.NoToken, new BinaryOperator(Token.NoToken, BinaryOperator.Opcode.Imp), new List<Expr> { identExp, expr });
                    var ens = new Ensures(false, impl);
                    
                    procedure.Ensures.Add(ens);
                    instCnt++;
                }
                instantiationKey += "\n";
            }

            // If we already computed the cost for this instantiation, no need to do it again.
            if (costCache.ContainsKey(file) && costCache[file].ContainsKey(instantiationKey))
            {
                cacheHits++;
                log("Template instantiation already processed before for this file; taking cost from there.");
                return costCache[file][instantiationKey];
            }
            // If we hit the bound for this instantiaton before, then it is ok to immediatelly
            // return "bound hit" if the new limit is smaller or equal to the saved one.
            else if (limit != 0 && costLimitCache.ContainsKey(file) &&
                      costLimitCache[file].ContainsKey(instantiationKey) &&
                      costLimitCache[file][instantiationKey] >= limit)
            {
                cacheHits++;
                log("Non-sufficient template instantiation already processed before; taking cost from there.");
                return null;
            }
            else
            {
                sdvCalls++;
            }

            log(string.Format("Annotated the program (of {0} methods) with {1} candidate instantiations.", instantiation.Keys.Count, instCnt));
            // Running Houdini and Corral must be done on fresh program instances.
            var pcopy1 = BoogieUtil.ReResolveInMem(prog);
            var pcopy2 = BoogieUtil.ReResolveInMem(prog);
            //cba.Util.BoogieUtil.PrintProgram(prog, "problem.bpl");

            var assignment = CoreLib.HoudiniInlining.RunHoudini(pcopy1, true);
            CoreLib.HoudiniInlining.InstrumentHoudiniAssignment(pcopy2, assignment);
            // Cost of Houdini is currently the number of thrown candidates.
            // This roughly corresponds to the number of solver calls, which
            // is what we ideally want.
            long houdiniCost = instCnt - assignment.Count;
            log("The number of thrown instantiations by Houdini is " + houdiniCost + ".");

            // Take a fresh copy for corral, just in case.
            var pcopy3 = BoogieUtil.ReResolveInMem(pcopy2);
            //cba.Util.BoogieUtil.PrintProgram(pcopy3, "interim2_" + call + ".bpl");
            // Run SI but first set bound to infinity.
            limit = limit < 0 ? 0 : limit;
            BoogieVerify.options.maxInlinedBound = limit;
            var err = new List<BoogieErrorTrace>();
            var rstatus = BoogieVerify.Verify(pcopy3, out err, true);

            var procs_inlined = BoogieVerify.CallTreeSize + 1;
            BoogieVerify.options.CallTree = new HashSet<string>();
            BoogieVerify.CallTreeSize = 0;
            BoogieVerify.verificationTime = TimeSpan.Zero;

            List<double> cost = null;
            if (rstatus == BoogieVerify.ReturnStatus.NOK)
            {
                throw new Exception("Corral returned NOT OK, we don't expect such benchmarks!");
            } else if (rstatus == BoogieVerify.ReturnStatus.ReachedBound)
            {
                cost = null;
            } else
            {
                cost = new List<double>();
                log(string.Format("Procedures inlined: {0}\tHoudini solver calls: {1}", procs_inlined, houdiniCost));
                //cost.Add(((double)(houdiniCost) / instantiation.Keys.Count) + procs_inlined);
                //cost.Add(hbalance * houdiniCost + procs_inlined);
                cost.Add(procs_inlined);
                cost.Add((int)((double)(houdiniCost) / instantiation.Keys.Count));
            }


            // If we hit the bound with an integer limit, then save this information separately.
            if (limit > 0 && cost == null)
            {
                if (!costLimitCache.ContainsKey(file))
                {
                    costLimitCache[file] = new Dictionary<string, int>();
                }
                costLimitCache[file][instantiationKey] = limit;
            }
            // Else cache the cost.
            else
            {
                if (!costCache.ContainsKey(file))
                {
                    costCache[file] = new Dictionary<string, List<double>>();
                }
                costCache[file][instantiationKey] = cost;
            }

            return cost;
        } 

        Dictionary<Procedure, List<Expr>> instantiateTemplate(string file, TemplateAnnotations tanns, ProgTransformation.PersistentProgram program)
        {
            // Get global variables.
            Program prog = program.getProgram();
            var globals = new Dictionary<string, Variable>();
            prog.TopLevelDeclarations
                .OfType<Variable>()
                .Iter(c => globals.Add(c.Name, c));

            var tannsCNF = tanns.ToCnfExpression();
            var annots = SimplifyExpr.GetExprConjunctions(tannsCNF);

            Dictionary<Procedure, List<Expr>> procToInsts = new Dictionary<Procedure, List<Expr>>();

            foreach (var impl in prog.TopLevelDeclarations.OfType<Implementation>())
            {
                var proc = impl.Proc;
                if (QKeyValue.FindBoolAttribute(impl.Attributes, "entrypoint")) continue;

                var formals = new Dictionary<string, Variable>();
                proc.InParams.OfType<Formal>()
                    .Iter(f => formals.Add(f.Name, f));
                proc.OutParams.OfType<Formal>()
                    .Iter(f => formals.Add(f.Name, f));

                HashSet<string> modifiesNames = new HashSet<string>();
                proc.Modifies.Iter(v => modifiesNames.Add(v.Name.ToString()));

                List<Expr> procInsts = new List<Expr>();
                for (int i = 0; i < annots.Count; i++) 
                {
                    Expr annot = annots[i];

                    // Get annotation with variable names.
                    GatherTemplateVariables tcollector = new GatherTemplateVariables();
                    tcollector.Visit(annot);
                    HashSet<Variable> templateVariables = tcollector.variables;

                    annot = getInstantationCandidate(annot, templateVariables, globals, formals, modifiesNames);
                    if (annot == null)
                    {
                        continue;
                    }

                    // If there are multiple template variables... 
                    List<Expr> insts = new List<Expr>();
                    if (templateVariables.Count > 0)
                    {
                        var strannot = annot.ToString();

                        if (!mdata.annotsToOrigins.ContainsKey(strannot))
                        {
                            log(string.Format("ERROR: template {0} origin missing postconditions.", annot.ToString()));
                        }

                        // Then...
                        if (mdata.annotsToOrigins.ContainsKey(strannot) && 
                            mdata.annotsToOrigins[strannot].ContainsKey(file) &&
                            mdata.annotsToOrigins[strannot][file].ContainsKey(proc.Name))
                        {
                            // We want to instantiate it only at the place of the origin and
                            // we want to take exactly the original annotation.
                            List<Expr> exprs = mdata.annotsToOrigins[strannot][file][proc.Name];
                            insts.AddRange(exprs);            
                        }
                    }
                    else
                    {
                        insts.Add(annot);
                    }
                    procInsts.AddRange(insts);
                }
                procToInsts[proc] = procInsts;
            }

            return procToInsts;
        }


        Expr getInstantationCandidate(Expr annotation, HashSet<Variable> annotationVars, Dictionary<string, Variable> globals, 
                                            Dictionary<string, Variable> formals, HashSet<string> modifiesNames)
        {
            // Get template variable names only.
            HashSet<string> annotationvarNames = new HashSet<string>();
            annotationVars.Iter(v => annotationvarNames.Add(v.Name));

            // Compute all variables in template that are not wrapped in "old(*)".
            var nonOldVisitor = new GatherNonOldVariables();
            nonOldVisitor.Visit(annotation);
            HashSet<string> nonOldVarNames = nonOldVisitor.variables;
            // Remove template variables. We now have (global) variables that are not wrapped with old.
            HashSet<string> nonOldGlobalVarNames = nonOldVarNames.Difference(annotationvarNames);
            
            // If there are some (global)) non-old variables and the method does not
            // modify some of them, then this template is of little or no use.
            if (nonOldGlobalVarNames.Count > 0 && !nonOldGlobalVarNames.IsSubsetOf(modifiesNames))
            {
                return null;
            }

            return annotation;
            //return MinControl.InstantiateTemplate(annotation, annotationVars, globals, formals);
        }

        string createIdentifier(string procName, string annot, int i, int j)
        {
            string ident = ART_VAR_PREFIX + procName.GetHashCode();
            ident += "_" + annot.GetHashCode();
            ident += "_" + i + "_" + j;
            ident = ident.Replace('-', '_');
            return ident;
        }

        Expr toUniqueVarTemplate(Expr expr, HashSet<Variable> templateVars)
        {
            Dictionary<Microsoft.Boogie.Type, int> inTypeIndices = new Dictionary<Microsoft.Boogie.Type, int>();
            Dictionary<Microsoft.Boogie.Type, int> outTypeIndices = new Dictionary<Microsoft.Boogie.Type, int>();
            var ret =
                Substituter.Apply(new Substitution(v =>
                {
                    if (v is Formal && (v as Formal).InComing)
                    {
                        var typ = v.TypedIdent.Type;
                        if (!inTypeIndices.ContainsKey(typ))
                        {
                            inTypeIndices[typ] = -1;
                        }
                        inTypeIndices[typ] += 1;
                        var nVar = BoogieAstFactory.MkFormal(v.Name + "_" + inTypeIndices[typ], typ, true);
                        templateVars.Add(nVar);
                        return Expr.Ident(nVar);
                    }
                    if (v is Formal && !(v as Formal).InComing)
                    {
                        var typ = v.TypedIdent.Type;
                        if (!outTypeIndices.ContainsKey(typ))
                        {
                            outTypeIndices[typ] = -1;
                        }
                        outTypeIndices[typ] += 1;
                        var nVar = BoogieAstFactory.MkFormal(v.Name + "_" + outTypeIndices[typ], typ, false);
                        templateVars.Add(nVar);
                        return Expr.Ident(nVar);
                    }
                    return Expr.Ident(v);
                }), expr);

            return ret;
        }



    }
}
