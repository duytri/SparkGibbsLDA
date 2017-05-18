package main.scala

import main.scala.obj.Model
import main.scala.obj.Dictionary
import main.scala.obj.Parameter
import main.scala.obj.LDADataset
import main.scala.connector.File2LDADataset
import scala.collection.mutable.ArrayBuffer
import main.scala.connector.Model2File

class Inferencer {
  // Train model
	var trnModel:Model =null
	var globalDict:Dictionary =null
	
	var newModel:Model =null
	val niters = 100
	
	//-----------------------------------------------------
	// Init method
	//-----------------------------------------------------
	def init(params:Parameter):Boolean={
		trnModel = new Model()
		
		if (!trnModel.initEstimatedModel(params))
			return false		
		
		globalDict = trnModel.data.localDict
		computeTrnTheta()
		computeTrnPhi()
		
		return true
	}
	
	//inference new model ~ getting data from a specified dataset
	def inference(newData:LDADataset,params:Parameter):Model={
		println("init new model")
		var newModel = new Model()		
		
		newModel.initNewModel(params, newData, trnModel)		
		this.newModel = newModel
		
		println("Sampling " + niters + " iteration for inference!");		
		for (iter <- 1 to niters){
			//System.out.println("Iteration " + newModel.liter + " ...");
			
			// for all newz_i
			for (m <- 0 until newModel.M){
				for (n <- 0 until newModel.data.docs(m).length){
					// (newz_i = newz[m][n]
					// sample from p(z_i|z_-1,w)
					val topic = infSampling(m, n)
					newModel.z(m).update(n, topic)
				}
			}//end foreach new doc
			
		}// end iterations
		
		println("Gibbs sampling for inference completed!")
		
		computeNewTheta()
		computeNewPhi()
		newModel.liter = niters-1
		return this.newModel
	}
	
	def inference(strs:ArrayBuffer[String],params: Parameter):Model={
		//System.out.println("inference");
		var newModel = new Model()
		
		//System.out.println("read dataset");
		val dataset = File2LDADataset.readDataSet(strs, globalDict);
		
		return inference(dataset, params)
	}
	
	//inference new model ~ getting dataset from file specified in option
	def inference(params:Parameter):Model={	
		//System.out.println("inference");
		
		newModel = new Model()
		if (!newModel.initNewModel(params, trnModel)) return null
		
		println("Sampling " + niters + " iteration for inference!")
		
		for (iter <- 1 to niters){
			//System.out.println("Iteration " + newModel.liter + " ...");
			
			// for all newz_i
			for (m <- 0 until newModel.M){
				for (n <- 0 until newModel.data.docs(m).length){
					// (newz_i = newz[m][n]
					// sample from p(z_i|z_-1,w)
					val topic = infSampling(m, n)
					newModel.z(m).update(n, topic)
				}
			}//end foreach new doc
			
		}// end iterations
		
		println("Gibbs sampling for inference completed!");		
		println("Saving the inference outputs!");
		
		computeNewTheta()
		computeNewPhi()
		newModel.liter = niters-1
		Model2File.saveModel(newModel.dfile + "." + newModel.modelName,newModel)
		
		return newModel
	}
	
	/**
	 * do sampling for inference
	 * m: document number
	 * n: word number?
	 */
	def infSampling(m:Int, n:Int):Int={
		// remove z_i from the count variables
		var topic = newModel.z(m)(n)
		val _w = newModel.data.docs(m).words(n)
		val w = newModel.data.lid2gid.get(_w).get
		newModel.nw(_w)(topic) -= 1
		newModel.nd(m)(topic) -= 1
		newModel.nwsum(topic) -= 1
		newModel.ndsum(m) -= 1
		
		val Vbeta = trnModel.V * newModel.beta;
		val Kalpha = trnModel.K * newModel.alpha;
		
		// do multinomial sampling via cummulative method		
		for (k <- 0 until newModel.K){			
			newModel.p(k) = (trnModel.nw(w)(k) + newModel.nw(_w)(k) + newModel.beta)/(trnModel.nwsum(k) +  newModel.nwsum(k) + Vbeta) *
					(newModel.nd(m)(k) + newModel.alpha)/(newModel.ndsum(m) + Kalpha)
		}
		
		// cummulate multinomial parameters
		for (k <- 1 until newModel.K){
			newModel.p(k) += newModel.p(k - 1)
		}
		
		// scaled sample because of unnormalized p[]
		val u = Math.random() * newModel.p(newModel.K - 1)
		
		topic = 0
    while (topic < newModel.K && newModel.p(topic) <= u) {
      topic += 1
    }
    if (topic == newModel.K) topic -= 1
		
		// add newly estimated z_i to count variables
		newModel.nw(_w)(topic) += 1
		newModel.nd(m)(topic) += 1
		newModel.nwsum(topic) += 1
		newModel.ndsum(m) += 1
		
		return topic
	}
	
	def computeNewTheta():Unit={
		for (m <- 0 until newModel.M){
			for (k <- 0 until newModel.K){
				newModel.theta(m)(k) = (newModel.nd(m)(k) + newModel.alpha) / (newModel.ndsum(m) + newModel.K * newModel.alpha)
			}//end foreach topic
		}//end foreach new document
	}
	
	def computeNewPhi():Unit={
		for (k <- 0 until newModel.K){
			for (_w <- 0 until newModel.V){
				val id = newModel.data.lid2gid.get(_w).get
				
				if (id != null){
					newModel.phi(k)(_w) = (trnModel.nw(id)(k) + newModel.nw(_w)(k) + newModel.beta) / (newModel.nwsum(k) + newModel.nwsum(k) + trnModel.V * newModel.beta)
				}
			}//end foreach word
		}// end foreach topic
	}
	
	def computeTrnTheta():Unit={
		for (m <- 0 until trnModel.M){
			for (k <- 0 until trnModel.K){
				trnModel.theta(m)(k) = (trnModel.nd(m)(k) + trnModel.alpha) / (trnModel.ndsum(m) + trnModel.K * trnModel.alpha)
			}
		}
	}
	
	def computeTrnPhi():Unit={
		for (k <- 0 until trnModel.K){
			for (w <- 0 until trnModel.V){
				trnModel.phi(k)(w) = (trnModel.nw(w)(k) + trnModel.beta) / (trnModel.nwsum(k) + trnModel.V * trnModel.beta)
			}
		}
	}
}