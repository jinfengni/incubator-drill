package org.apache.drill.exec.resolver;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public class OperatorFunctionResolver implements FunctionResolver {

	@Override
	public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods, FunctionCall call) {
	
	  int bestcost = Integer.MAX_VALUE;
		int currcost = Integer.MAX_VALUE;
		DrillFuncHolder bestmatch = null;
		
		for (DrillFuncHolder h : methods) {
			currcost = TypeCastRules.getCost(call, h);
			
			// if cost is lower than 0, func implementation is not matched, either w/ or w/o implicit casts
			if (currcost  < 0 ){				
				continue;
			}
			
			if (currcost < bestcost) {
				bestcost = currcost;
				bestmatch = h;
			}	      
		}
			      
		if (bestcost < 0) {
		  //did not find a matched func implementation, either w/ or w/o implicit casts
		  //TODO: raise exception here?
		  return null;
		} else
		  return bestmatch;
	}

}
