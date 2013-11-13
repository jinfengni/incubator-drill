package org.apache.drill.exec.resolver;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import com.google.common.collect.ImmutableList;

public class OperatorFunctionResolver implements FunctionResolver {

	@Override
	public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods, FunctionCall call) {
		
		ImmutableList<LogicalExpression> args = call.args;
		
		if(args.size() != 2){
			// TODO: Some Exception
		}		
		
		MajorType m1 = args.get(0).getMajorType();
		MajorType m2 = args.get(1).getMajorType();
		
		Integer t1 = ResolverTypePrecedence.precedenceMap.get(args.get(0).getMajorType().getMinorType().name()); //BIGINT, REQUIRED
		Integer t2 = ResolverTypePrecedence.precedenceMap.get(args.get(1).getMajorType().getMinorType().name()); // FLOAT8, REQUIRED
		
		if(t1==null || t2==null){
			// TODO: precedence not defined in MAP
		}
		else{
			MajorType commonType = (t1 > t2) ? args.get(0).getMajorType() : args.get(1).getMajorType();
		}
		
		/** Create New arguments **/
		//List<LogicalExpression> newargs = Lists.newArrayList();
	     // for (int i = 0; i < call.args.size(); ++i) {
	     //   LogicalExpression newExpr = // TODO
	     //   args.add(newExpr);
	     // }
		
		/** Modify function call **/
		//FunctionCall modifiedCall = new FunctionCall(call.getDefinition(), newargs, call.getPosition());
		 
		
		for(DrillFuncHolder h : methods){
		      if(h.matches(call)){
		        return h;
		      }
		    }
		
		return null;
	}

}
